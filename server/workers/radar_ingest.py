

import json
import logging
from pathlib import Path

from server.providers import create_provider, ScanEntry
from server.data.radar import decode_level2_file
from server.data.renderer import render_sweep_to_png
from server.data.colormaps import load_colormaps
from server.cache.db import CacheDB
from server.cache.tiles import TileCache

logger = logging.getLogger(__name__)

PRODUCT_COLORMAPS = {
    "reflectivity": "nws_reflectivity",
    "velocity": "nws_velocity",
}


def load_sites(path: str = "config/nexrad_sites.json") -> list[dict]:
    with open(path) as f:
        return json.load(f)


def list_recent_scans(site: str, count: int = 20) -> list[ScanEntry]:
    """List recent scans using the provider chain (chunks -> volume)."""
    provider = create_provider(site)
    return provider.list_scans(count=count)


def ingest_scan(
    site: str,
    scan_entry: ScanEntry,
    db: CacheDB,
    tile_cache: TileCache,
    download_dir: str,
) -> dict:
    """Full ingest pipeline: download -> decode -> render -> cache."""
    provider = create_provider(site)
    local_path = provider.download(scan_entry, download_dir)
    scan_time_str = scan_entry.scan_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    scan_id = db.insert_scan(site, scan_time_str, local_path)
    sweeps = decode_level2_file(local_path, site_id=site)

    rendered = []
    for sweep in sweeps:
        if sweep.sweep != 0:
            continue
        colormap_name = PRODUCT_COLORMAPS.get(sweep.product)
        if not colormap_name:
            continue

        result = render_sweep_to_png(sweep, colormap_name)
        image_path = tile_cache.get_image_path(
            site, scan_time_str, sweep.product, sweep.sweep
        )
        tile_cache.save_image(image_path, result.image_data)

        db.insert_rendered_image(
            scan_id=scan_id,
            product=sweep.product,
            sweep=sweep.sweep,
            image_path=str(image_path),
            bounds=result.bounds,
        )

        rendered.append({
            "product": sweep.product,
            "sweep": sweep.sweep,
            "bounds": result.bounds,
        })

    logger.info(
        f"Ingested {site} scan at {scan_time_str} via {scan_entry.source}: "
        f"{len(rendered)} products"
    )
    return {
        "site": site,
        "scan_time": scan_time_str,
        "products": rendered,
    }
