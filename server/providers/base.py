
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path


@dataclass
class ScanEntry:
    site: str
    scan_time: datetime
    s3_key: str
    source: str          # "chunks" or "volume"
    size: int = 0
    is_complete: bool = True  # False for in-progress chunk scans


@dataclass
class ProviderState:
    last_scan_time: datetime | None = None
    last_check_time: datetime | None = None
    scan_interval_seconds: float = 300.0  # estimated time between scans
    consecutive_empty: int = 0
    fast_retry_seconds: float = 15.0
    slow_retry_seconds: float = 120.0


class NexradProvider(ABC):


    BUCKET: str = ""
    REGION: str = "us-east-1"

    def __init__(self, site: str):
        self.site = site
        self.state = ProviderState()
        self._s3 = None

    def _get_s3(self):
        if self._s3 is None:
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config
            self._s3 = boto3.client(
                "s3",
                region_name=self.REGION,
                config=Config(
                    signature_version=UNSIGNED,
                    connect_timeout=10,
                    read_timeout=30,
                ),
            )
        return self._s3

    @abstractmethod
    def list_scans(self, count: int = 20) -> list[ScanEntry]:

    @abstractmethod
    def download(self, entry: ScanEntry, dest_dir: str) -> str:

    def next_poll_seconds(self) -> float:

        if self.state.last_scan_time is None:
            return self.state.fast_retry_seconds

        from datetime import timezone
        now = datetime.now(timezone.utc)
        last = self.state.last_scan_time
        # Ensure both are timezone-aware for comparison
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        since_last = (now - last).total_seconds()

        if since_last > self.state.scan_interval_seconds * 5:
            return self.state.slow_retry_seconds

        # Estimate when next scan arrives
        time_until_next = self.state.scan_interval_seconds - since_last
        if time_until_next <= 0:
            return self.state.fast_retry_seconds

        return min(time_until_next, self.state.slow_retry_seconds)

    def update_timing(self, new_scan_time: datetime):
        """Update adaptive timing after discovering a new scan."""
        if self.state.last_scan_time and new_scan_time > self.state.last_scan_time:
            interval = (new_scan_time - self.state.last_scan_time).total_seconds()
            # Exponential moving average
            self.state.scan_interval_seconds = (
                0.7 * self.state.scan_interval_seconds + 0.3 * interval
            )
        self.state.last_scan_time = new_scan_time
        self.state.consecutive_empty = 0
