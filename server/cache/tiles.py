from pathlib import Path
import time


class TileCache:
    def __init__(self, cache_dir: str):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_image_path(self, site: str, scan_time: str, product: str, sweep: int) -> Path:
        date = scan_time[:10]
        time_part = scan_time[11:19].replace(":", "")
        return self.cache_dir / site / date / f"{product}_s{sweep}_{time_part}.png"

    def save_image(self, path: Path, image_data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(image_data)

    def load_image(self, path: Path) -> bytes | None:
        if path.exists():
            return path.read_bytes()
        return None

    def delete_images(self, image_paths: list[str]) -> int:
        removed = 0
        for image_path in image_paths:
            path = Path(image_path)
            if path.exists():
                path.unlink()
                removed += 1

            parent = path.parent
            while parent != self.cache_dir and parent.exists():
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
        return removed

    def cleanup(self, max_age_minutes: int) -> int:
        cutoff = time.time() - (max_age_minutes * 60)
        removed = 0
        for png in self.cache_dir.rglob("*.png"):
            if png.stat().st_mtime < cutoff:
                png.unlink()
                removed += 1
        for d in sorted(self.cache_dir.rglob("*"), reverse=True):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
        return removed
