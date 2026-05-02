"""Provider factory — mirrors supercell-wx's NexradDataProviderFactory.

Creates the appropriate provider chain for a radar site:
1. ChunksProvider for real-time data (~30s latency)
2. VolumeProvider as fallback for completed scans

The factory also manages per-site provider instances so polling
state (adaptive intervals) persists across requests.
"""

import logging
from .base import NexradProvider, ScanEntry
from .chunks import ChunksProvider
from .volume import VolumeProvider

logger = logging.getLogger(__name__)

# Cache of provider instances per site
_providers: dict[str, "CompositeProvider"] = {}


class CompositeProvider(NexradProvider):
    """Tries chunks first (real-time), falls back to volume (archive).

    This mirrors supercell-wx's pattern where the chunks provider
    is primary for the current scan, and the volume provider fills
    in completed previous scans.
    """

    BUCKET = ""  # Not used directly

    def __init__(self, site: str):
        super().__init__(site)
        self.chunks = ChunksProvider(site)
        self.volume = VolumeProvider(site)

    def list_scans(self, count: int = 20) -> list[ScanEntry]:
        """List scans from both providers, deduplicated by time."""
        # Try chunks first for real-time data
        chunk_scans = []
        try:
            chunk_scans = self.chunks.list_scans(count=count)
            if chunk_scans:
                logger.info(
                    f"Found {len(chunk_scans)} chunk scans for {self.site}"
                )
        except Exception as e:
            logger.warning(f"Chunks provider failed for {self.site}: {e}")

        # Also get volume scans for history
        volume_scans = []
        try:
            volume_scans = self.volume.list_scans(count=count)
            if volume_scans:
                logger.info(
                    f"Found {len(volume_scans)} volume scans for {self.site}"
                )
        except Exception as e:
            logger.warning(f"Volume provider failed for {self.site}: {e}")

        # Merge: prefer volume over chunks for the same time
        # (volume files are complete, chunks may be partial)
        by_time: dict[str, ScanEntry] = {}
        for entry in chunk_scans:
            key = entry.scan_time.strftime("%Y%m%d%H%M%S")
            by_time[key] = entry
        for entry in volume_scans:
            key = entry.scan_time.strftime("%Y%m%d%H%M%S")
            # Volume overwrites chunks (more reliable)
            by_time[key] = entry

        merged = sorted(by_time.values(), key=lambda e: e.scan_time, reverse=True)

        if merged:
            self.update_timing(merged[0].scan_time)

        return merged[:count]

    def download(self, entry: ScanEntry, dest_dir: str) -> str:
        """Route download to the appropriate provider."""
        if entry.source == "chunks":
            return self.chunks.download(entry, dest_dir)
        else:
            return self.volume.download(entry, dest_dir)

    def next_poll_seconds(self) -> float:
        """Use the faster of the two providers' intervals."""
        return min(
            self.chunks.next_poll_seconds(),
            self.volume.next_poll_seconds(),
        )


def create_provider(site: str) -> CompositeProvider:
    """Get or create a provider for a site (cached for polling state)."""
    if site not in _providers:
        _providers[site] = CompositeProvider(site)
    return _providers[site]
