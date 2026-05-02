import sqlite3
from pathlib import Path


class CacheDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS radar_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site TEXT NOT NULL,
                scan_time TEXT NOT NULL,
                volume_file TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(site, scan_time)
            );
            CREATE TABLE IF NOT EXISTS rendered_images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL REFERENCES radar_scans(id),
                product TEXT NOT NULL,
                sweep INTEGER NOT NULL,
                image_path TEXT NOT NULL,
                bounds_north REAL NOT NULL,
                bounds_south REAL NOT NULL,
                bounds_east REAL NOT NULL,
                bounds_west REAL NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(scan_id, product, sweep)
            );
        """)
        self.conn.commit()

    def insert_scan(self, site: str, scan_time: str, volume_file: str) -> int:
        cursor = self.conn.execute(
            "SELECT id FROM radar_scans WHERE site = ? AND scan_time = ?",
            (site, scan_time),
        )
        row = cursor.fetchone()
        if row:
            return row["id"]
        cursor = self.conn.execute(
            "INSERT INTO radar_scans (site, scan_time, volume_file) VALUES (?, ?, ?)",
            (site, scan_time, volume_file),
        )
        self.conn.commit()
        return cursor.lastrowid

    def list_scans(self, site: str, limit: int = 50) -> list[dict]:
        cursor = self.conn.execute(
            "SELECT * FROM radar_scans WHERE site = ? ORDER BY scan_time DESC LIMIT ?",
            (site, limit),
        )
        return [dict(row) for row in cursor.fetchall()]

    def count_scans(self, site: str) -> int:
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM radar_scans WHERE site = ?",
            (site,),
        )
        return int(cursor.fetchone()[0])

    def get_latest_scan(self, site: str) -> dict | None:
        cursor = self.conn.execute(
            "SELECT * FROM radar_scans WHERE site = ? ORDER BY scan_time DESC LIMIT 1",
            (site,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def insert_rendered_image(self, scan_id: int, product: str, sweep: int,
                              image_path: str, bounds: dict) -> int:
        cursor = self.conn.execute(
            """INSERT OR REPLACE INTO rendered_images
               (scan_id, product, sweep, image_path,
                bounds_north, bounds_south, bounds_east, bounds_west)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (scan_id, product, sweep, image_path,
             bounds["north"], bounds["south"], bounds["east"], bounds["west"]),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_rendered_image(self, scan_id: int, product: str, sweep: int) -> dict | None:
        cursor = self.conn.execute(
            "SELECT * FROM rendered_images WHERE scan_id = ? AND product = ? AND sweep = ?",
            (scan_id, product, sweep),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_scan_images(self, scan_id: int) -> list[dict]:
        cursor = self.conn.execute(
            "SELECT * FROM rendered_images WHERE scan_id = ?", (scan_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def store_scan_bundle(
        self,
        site: str,
        scan_time: str,
        volume_file: str,
        rendered_images: list[dict],
    ) -> int:
        cursor = self.conn.execute(
            "SELECT id FROM radar_scans WHERE site = ? AND scan_time = ?",
            (site, scan_time),
        )
        row = cursor.fetchone()
        if row:
            scan_id = row["id"]
        else:
            cursor = self.conn.execute(
                "INSERT INTO radar_scans (site, scan_time, volume_file) VALUES (?, ?, ?)",
                (site, scan_time, volume_file),
            )
            scan_id = cursor.lastrowid

        for image in rendered_images:
            self.conn.execute(
                """INSERT OR REPLACE INTO rendered_images
                   (scan_id, product, sweep, image_path,
                    bounds_north, bounds_south, bounds_east, bounds_west)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    scan_id,
                    image["product"],
                    image["sweep"],
                    image["image_path"],
                    image["bounds"]["north"],
                    image["bounds"]["south"],
                    image["bounds"]["east"],
                    image["bounds"]["west"],
                ),
            )

        self.conn.commit()
        return scan_id

    def prune_scans(self, site: str, keep: int) -> list[dict]:
        if keep < 0:
            keep = 0

        cursor = self.conn.execute(
            """
            SELECT id, scan_time
            FROM radar_scans
            WHERE site = ?
            ORDER BY scan_time DESC
            LIMIT -1 OFFSET ?
            """,
            (site, keep),
        )
        scans_to_delete = [dict(row) for row in cursor.fetchall()]

        removed = []
        for scan in scans_to_delete:
            image_rows = self.get_scan_images(scan["id"])
            self.conn.execute("DELETE FROM rendered_images WHERE scan_id = ?", (scan["id"],))
            self.conn.execute("DELETE FROM radar_scans WHERE id = ?", (scan["id"],))
            removed.append({
                "scan_id": scan["id"],
                "scan_time": scan["scan_time"],
                "image_paths": [row["image_path"] for row in image_rows],
            })

        if removed:
            self.conn.commit()
        return removed

    def delete_scan(self, scan_id: int):
        self.conn.execute("DELETE FROM rendered_images WHERE scan_id = ?", (scan_id,))
        self.conn.execute("DELETE FROM radar_scans WHERE id = ?", (scan_id,))
        self.conn.commit()

    def close(self):
        self.conn.close()
