import asyncio
import json
import logging
from pathlib import Path
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import Response

from server.cache.db import CacheDB
from server.cache.tiles import TileCache
from server.config import get_config

logger = logging.getLogger(__name__)
router = APIRouter()

SITES_PATH = "config/nexrad_sites.json"
MIN_SCAN_COUNT = 1
MAX_SCAN_COUNT = 120

# Global state — initialized by app lifespan
_db: CacheDB | None = None
_tile_cache: TileCache | None = None
_active_connections: set[WebSocket] = set()
_active_ingests: dict[str, asyncio.Task] = {}


def init_globals(db: CacheDB, tile_cache: TileCache):
    global _db, _tile_cache
    _db = db
    _tile_cache = tile_cache


def get_db() -> CacheDB:
    return _db


def get_tile_cache() -> TileCache:
    return _tile_cache


def _clamp_scan_count(value: int | None, default: int) -> int:
    if value is None:
        value = default
    return max(MIN_SCAN_COUNT, min(MAX_SCAN_COUNT, int(value)))


def _resolve_scan_request(count: int | None, max_scans: int | None) -> tuple[int, int, int]:
    config = get_config()
    radar_config = config.get("radar", {})
    poll_interval = max(5, int(radar_config.get("poll_interval_seconds", 120)))

    initial_count = _clamp_scan_count(
        count,
        radar_config.get("initial_scan_count", 30),
    )
    resolved_max = _clamp_scan_count(
        max_scans,
        radar_config.get("max_scans", 120),
    )
    resolved_max = max(initial_count, resolved_max)
    return initial_count, resolved_max, poll_interval


def _clear_ingest_task(site: str, task: asyncio.Task):
    current = _active_ingests.get(site)
    if current is task:
        _active_ingests.pop(site, None)
    try:
        task.result()
    except Exception:
        logger.exception("[ingest] Task failed for %s", site)


# --- REST Endpoints ---

@router.get("/settings")
async def get_radar_settings():
    initial_count, max_scans, poll_interval = _resolve_scan_request(None, None)
    return {
        "poll_interval_seconds": poll_interval,
        "default_initial_scan_count": initial_count,
        "default_max_scans": max_scans,
        "max_allowed_scans": MAX_SCAN_COUNT,
    }


@router.get("/sites")
async def list_sites():
    with open(SITES_PATH) as f:
        return json.load(f)


@router.get("/{site}/scans")
async def list_scans(site: str, limit: int = 50):
    db = get_db()
    limit = max(MIN_SCAN_COUNT, min(MAX_SCAN_COUNT, limit))
    return db.list_scans(site, limit)


@router.get("/{site}/status")
async def get_site_status(site: str):
    db = get_db()
    latest = db.get_latest_scan(site)
    initial_count, max_scans, poll_interval = _resolve_scan_request(None, None)
    task = _active_ingests.get(site)
    running = bool(task and not task.done())
    return {
        "site": site,
        "running": running,
        "cached_scan_count": db.count_scans(site),
        "latest_scan_time": latest["scan_time"] if latest else None,
        "poll_interval_seconds": poll_interval,
        "default_initial_scan_count": initial_count,
        "default_max_scans": max_scans,
        "max_allowed_scans": MAX_SCAN_COUNT,
    }


@router.get("/{site}/latest")
async def get_latest_scan(site: str):
    db = get_db()
    scan = db.get_latest_scan(site)
    if not scan:
        raise HTTPException(404, f"No scans cached for {site}")
    images = db.get_scan_images(scan["id"])
    return {
        "scan": scan,
        "images": images,
    }


@router.get("/{site}/latest/{product}/{sweep}/image")
async def get_latest_image(site: str, product: str, sweep: int):
    db = get_db()
    tc = get_tile_cache()
    scan = db.get_latest_scan(site)
    if not scan:
        raise HTTPException(404, f"No scans cached for {site}")
    img = db.get_rendered_image(scan["id"], product, sweep)
    if not img:
        raise HTTPException(404, f"No rendered image for {product} sweep {sweep}")
    image_data = tc.load_image(Path(img["image_path"]))
    if not image_data:
        raise HTTPException(404, "Image file not found in cache")
    return Response(content=image_data, media_type="image/png")


@router.get("/{site}/scan/{scan_time}/{product}/{sweep}/image")
async def get_scan_image(site: str, scan_time: str, product: str, sweep: int):
    db = get_db()
    tc = get_tile_cache()
    scans = db.list_scans(site)
    scan = next((s for s in scans if s["scan_time"] == scan_time), None)
    if not scan:
        raise HTTPException(404, f"Scan not found: {site} at {scan_time}")
    img = db.get_rendered_image(scan["id"], product, sweep)
    if not img:
        raise HTTPException(404, f"No rendered image for {product} sweep {sweep}")
    image_data = tc.load_image(Path(img["image_path"]))
    if not image_data:
        raise HTTPException(404, "Image file not found in cache")
    return Response(content=image_data, media_type="image/png")


@router.get("/{site}/scan/{scan_time}/{product}/{sweep}/meta")
async def get_scan_image_meta(site: str, scan_time: str, product: str, sweep: int):
    db = get_db()
    scans = db.list_scans(site)
    scan = next((s for s in scans if s["scan_time"] == scan_time), None)
    if not scan:
        raise HTTPException(404, f"Scan not found: {site} at {scan_time}")
    img = db.get_rendered_image(scan["id"], product, sweep)
    if not img:
        raise HTTPException(404, f"No rendered image for {product} sweep {sweep}")
    return {
        "site": site,
        "scan_time": scan_time,
        "product": product,
        "sweep": sweep,
        "bounds": {
            "north": img["bounds_north"],
            "south": img["bounds_south"],
            "east": img["bounds_east"],
            "west": img["bounds_west"],
        },
        "image_url": f"/api/radar/{site}/scan/{scan_time}/{product}/{sweep}/image",
    }


@router.post("/{site}/fetch")
async def trigger_fetch(site: str, count: int | None = None, max_scans: int | None = None):
    """Trigger fetching recent radar data for a site without overlapping runs."""
    requested_count, resolved_max_scans, poll_interval = _resolve_scan_request(count, max_scans)

    task = _active_ingests.get(site)
    if task and not task.done():
        return {
            "status": "running",
            "site": site,
            "count": requested_count,
            "max_scans": resolved_max_scans,
            "poll_interval_seconds": poll_interval,
        }

    task = asyncio.create_task(
        asyncio.to_thread(_run_ingest, site, requested_count, resolved_max_scans)
    )
    _active_ingests[site] = task
    task.add_done_callback(lambda completed, site=site: _clear_ingest_task(site, completed))
    return {
        "status": "started",
        "site": site,
        "count": requested_count,
        "max_scans": resolved_max_scans,
        "poll_interval_seconds": poll_interval,
    }


def _run_ingest(site: str, requested_count: int = 30, max_scans: int = 120):
    """Fetch recent scans for a site, add only new ones, then prune to a rolling max."""
    import tempfile
    from server.workers.radar_ingest import list_recent_scans, ingest_scan
    from server.data.colormaps import load_colormaps as init_colormaps

    # Thread-local DB connection (SQLite can't share across threads)
    config = get_config()
    cache_dir = config.get("cache", {}).get("directory", "cache")
    db = CacheDB(f"{cache_dir}/opengr.db")
    tc = TileCache(cache_dir)

    try:
        init_colormaps()
        logger.info(
            "[ingest] Fetching up to %s recent scans for %s (rolling max %s)",
            requested_count,
            site,
            max_scans,
        )
        scans = list_recent_scans(site, count=requested_count)
        if not scans:
            logger.warning(f"[ingest] No recent scans found for {site}")
            return

        # Find which scans we already have
        existing_scans = db.list_scans(site, limit=max_scans)
        existing_times = {s["scan_time"] for s in existing_scans}

        # Filter to only new scans (scans are ScanEntry objects)
        new_scans = [
            s for s in scans
            if s.scan_time.strftime("%Y-%m-%dT%H:%M:%SZ") not in existing_times
        ]

        if not new_scans:
            logger.info(f"[ingest] Already up to date for {site} ({len(existing_scans)} cached)")
            return

        logger.info(f"[ingest] {len(new_scans)} new scans to download for {site}")

        # Download newest first so the user sees data ASAP
        for i, scan_entry in enumerate(new_scans):
            try:
                label = f"{scan_entry.source}:{scan_entry.s3_key.split('/')[-1]}"
                logger.info(f"[ingest] [{i+1}/{len(new_scans)}] Downloading {label}...")
                with tempfile.TemporaryDirectory() as tmpdir:
                    result = ingest_scan(site, scan_entry, db, tc, tmpdir)
                logger.info(f"[ingest] [{i+1}/{len(new_scans)}] Done: {result['scan_time']}")
            except Exception as e:
                logger.error(f"[ingest] Failed on {scan_entry.s3_key}: {e}")
                continue

        logger.info(
            "[ingest] Finished %s: %s new + %s existing",
            site,
            len(new_scans),
            len(existing_scans),
        )
    except Exception as e:
        logger.error(f"[ingest] FAILED for {site}: {e}", exc_info=True)
    finally:
        db.close()


# --- WebSocket ---

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _active_connections.add(websocket)
    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        _active_connections.discard(websocket)


async def broadcast_message(message: dict):
    for ws in _active_connections.copy():
        try:
            await ws.send_json(message)
        except Exception:
            _active_connections.discard(ws)
