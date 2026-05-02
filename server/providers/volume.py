"""Completed volume scan provider.

Mirrors supercell-wx's AwsLevel2DataProvider:
- Bucket: unidata-nexrad-level2
- Full volume scans (all sweeps in one file)
- Key format: {YYYY}/{MM}/{DD}/{SITE}/{SITE}{YYYYMMDD}_{HHMMSS}[_V0X][.gz]
- Files may be gzip-compressed
"""

import gzip
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .base import NexradProvider, ScanEntry

logger = logging.getLogger(__name__)


class VolumeProvider(NexradProvider):
    """Completed NEXRAD Level 2 volume scans from Unidata S3."""

    BUCKET = "unidata-nexrad-level2"

    def __init__(self, site: str):
        super().__init__(site)
        self.state.fast_retry_seconds = 15.0   # supercell-wx uses 15s
        self.state.slow_retry_seconds = 120.0  # 2min slow retry

    def list_scans(self, count: int = 20) -> list[ScanEntry]:
        """List recent volume scans, checking today and yesterday."""
        s3 = self._get_s3()
        now = datetime.now(timezone.utc)

        entries = []
        for day_offset in range(2):
            d = now - timedelta(days=day_offset)
            prefix = f"{d.year}/{d.month:02d}/{d.day:02d}/{self.site}/"

            try:
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.BUCKET, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        entry = self._parse_object(obj)
                        if entry:
                            entries.append(entry)
            except Exception as e:
                logger.warning(f"Volume listing failed for {prefix}: {e}")

        entries.sort(key=lambda e: e.scan_time, reverse=True)

        if entries:
            self.update_timing(entries[0].scan_time)

        return entries[:count]

    def _parse_object(self, obj: dict) -> ScanEntry | None:
        """Parse an S3 object into a ScanEntry."""
        key = obj["Key"]
        filename = key.split("/")[-1]

        # Skip MDM (metadata) files and tiny files
        if "_MDM" in filename or obj["Size"] < 100_000:
            return None

        # Parse time from filename: SITE + YYYYMMDD_HHMMSS
        name = filename.replace(".gz", "")
        try:
            date_str = name[4:12]   # YYYYMMDD
            time_str = name[13:19]  # HHMMSS
            scan_time = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        except (ValueError, IndexError):
            return None

        return ScanEntry(
            site=self.site,
            scan_time=scan_time,
            s3_key=key,
            source="volume",
            size=obj["Size"],
            is_complete=True,
        )

    def download(self, entry: ScanEntry, dest_dir: str) -> str:
        """Download a volume scan, decompressing gzip if needed."""
        s3 = self._get_s3()
        filename = entry.s3_key.split("/")[-1]
        dest_name = filename.replace(".gz", "")
        dest_path = Path(dest_dir) / dest_name
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Download
        tmp_path = Path(dest_dir) / filename
        s3.download_file(self.BUCKET, entry.s3_key, str(tmp_path))

        # Decompress if gzipped
        if filename.endswith(".gz"):
            with gzip.open(tmp_path, "rb") as f_in:
                dest_path.write_bytes(f_in.read())
            tmp_path.unlink()
        elif tmp_path != dest_path:
            tmp_path.rename(dest_path)

        logger.info(
            f"Downloaded volume {self.site} {entry.scan_time} "
            f"({dest_path.stat().st_size} bytes)"
        )
        return str(dest_path)
