"""Real-time chunked NEXRAD data provider.

Mirrors supercell-wx's AwsLevel2ChunksDataProvider:
- Bucket: unidata-nexrad-level2-chunks
- Scans arrive as individual chunks (S=Start, I=Intermediate, E=End)
- ~30 second latency from radar to availability
- Key format: {SITE}/{ScanNumber}/{YYYYMMDD}-{HHMMSS}-{NNN}-{Type}
"""

import gzip
import logging
import tempfile
from datetime import datetime
from pathlib import Path

from .base import NexradProvider, ScanEntry

logger = logging.getLogger(__name__)


class ChunksProvider(NexradProvider):
    """Near real-time radar data via LDM-style chunks on S3."""

    BUCKET = "unidata-nexrad-level2-chunks"

    def __init__(self, site: str):
        super().__init__(site)
        self.state.fast_retry_seconds = 3.0   # supercell-wx uses 3s for chunks
        self.state.slow_retry_seconds = 20.0  # 20s slow retry

    def list_scans(self, count: int = 20) -> list[ScanEntry]:
        """List recent scan directories from the chunks bucket.

        The bucket structure is:
          {SITE}/ -> common prefixes like {SITE}/585/, {SITE}/586/
        Each prefix is a scan number containing chunk files.
        """
        s3 = self._get_s3()
        prefix = f"{self.site}/"

        try:
            # List scan directories using delimiter
            response = s3.list_objects_v2(
                Bucket=self.BUCKET,
                Prefix=prefix,
                Delimiter="/",
            )
        except Exception as e:
            logger.warning(f"Chunks listing failed for {self.site}: {e}")
            return []

        scan_dirs = [
            cp["Prefix"] for cp in response.get("CommonPrefixes", [])
        ]
        if not scan_dirs:
            self.state.consecutive_empty += 1
            return []

        # Get the most recent scan directories (highest scan numbers)
        # Scan numbers wrap around 1-999
        scan_dirs.sort(key=lambda p: int(p.rstrip("/").split("/")[-1]))

        # Find the latest scans by detecting gaps in sequence
        # (supercell-wx looks for gaps to identify the current scan boundary)
        recent_dirs = scan_dirs[-min(count, len(scan_dirs)):]

        entries = []
        for scan_dir in reversed(recent_dirs):
            scan_entry = self._probe_scan_dir(scan_dir)
            if scan_entry:
                entries.append(scan_entry)

        return entries

    def _probe_scan_dir(self, scan_dir: str) -> ScanEntry | None:
        """Probe a scan directory to get its timestamp and completeness."""
        s3 = self._get_s3()
        try:
            response = s3.list_objects_v2(
                Bucket=self.BUCKET,
                Prefix=scan_dir,
                MaxKeys=100,
            )
        except Exception as e:
            logger.warning(f"Failed to probe {scan_dir}: {e}")
            return None

        contents = response.get("Contents", [])
        if not contents:
            return None

        # Parse chunk info from keys
        # Format: {SITE}/{ScanNum}/{YYYYMMDD}-{HHMMSS}-{NNN}-{Type}
        chunks = []
        scan_time = None
        has_start = False
        has_end = False
        total_size = 0

        for obj in contents:
            key = obj["Key"]
            filename = key.split("/")[-1]
            parts = filename.split("-")
            if len(parts) < 4:
                continue

            chunk_type = parts[-1]  # S, I, or E
            has_start = has_start or chunk_type == "S"
            has_end = has_end or chunk_type == "E"
            total_size += obj["Size"]

            if scan_time is None:
                try:
                    date_str = parts[0]  # YYYYMMDD
                    time_str = parts[1]  # HHMMSS
                    scan_time = datetime.strptime(
                        f"{date_str}{time_str}", "%Y%m%d%H%M%S"
                    )
                except (ValueError, IndexError):
                    continue

            chunks.append({"key": key, "type": chunk_type, "size": obj["Size"]})

        if not scan_time or not chunks:
            return None

        return ScanEntry(
            site=self.site,
            scan_time=scan_time,
            s3_key=scan_dir.rstrip("/"),  # The scan directory
            source="chunks",
            size=total_size,
            is_complete=(has_start and has_end),
        )

    def download(self, entry: ScanEntry, dest_dir: str) -> str:
        """Download all chunks for a scan and concatenate into a volume file.

        supercell-wx loads chunks individually and feeds them to Ar2vFile:
        - S chunk: volume header + first bzip2 records
        - I chunks: additional bzip2 records
        - E chunk: final bzip2 records

        We concatenate them into a single file that pyart can read.
        """
        s3 = self._get_s3()
        scan_dir = entry.s3_key + "/"

        try:
            response = s3.list_objects_v2(
                Bucket=self.BUCKET,
                Prefix=scan_dir,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to list chunks for {entry.s3_key}: {e}")

        contents = response.get("Contents", [])
        if not contents:
            raise RuntimeError(f"No chunks found in {entry.s3_key}")

        # Sort chunks by sequence number (the NNN part)
        def chunk_sort_key(obj):
            filename = obj["Key"].split("/")[-1]
            parts = filename.split("-")
            # Type priority: S=0, I=1, E=2
            type_order = {"S": 0, "I": 1, "E": 2}
            chunk_num = int(parts[2]) if len(parts) > 2 else 0
            chunk_type = parts[-1] if parts else "I"
            return (chunk_num, type_order.get(chunk_type, 1))

        contents.sort(key=chunk_sort_key)

        # Download and concatenate all chunks
        scan_num = entry.s3_key.split("/")[-1]
        dest_path = Path(dest_dir) / f"{self.site}_{scan_num}_chunks.ar2v"
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "wb") as outfile:
            for obj in contents:
                chunk_data = s3.get_object(Bucket=self.BUCKET, Key=obj["Key"])
                outfile.write(chunk_data["Body"].read())

        logger.info(
            f"Downloaded {len(contents)} chunks for {self.site} scan {scan_num} "
            f"({dest_path.stat().st_size} bytes)"
        )
        return str(dest_path)
