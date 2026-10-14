"""Single-writer, batch-only durable recovery state."""

from __future__ import annotations

import sqlite3
import time
import uuid


def now_ms():
    return time.time_ns() // 1_000_000


class BatchStore:
    def __init__(self, config):
        self.db = sqlite3.connect(config.work_dir / "batches.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS batches (
                id TEXT PRIMARY KEY, path TEXT UNIQUE NOT NULL, asset_path TEXT NOT NULL,
                timestamp INTEGER NOT NULL, task_type TEXT NOT NULL, collect_method TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0,
                retry_at REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
                cleanup_pending INTEGER NOT NULL DEFAULT 0, pending BLOB,
                files INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0
            );
        """)
        for key, value in (("source_dir", str(config.source_dir)), ("obs_path", config.obs_path)):
            row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            if row and row[0] != value:
                self.db.close()
                raise ValueError(f"工作目录绑定的 {key} 不同，请使用新的工作目录")
            self.db.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (key, value))
        self.db.commit()

    def add(self, path, config):
        asset_path = config.obs_path + (path.rstrip("/") + "/" if path != "." else "")
        with self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO batches
                (id,path,asset_path,timestamp,task_type,collect_method) VALUES (?,?,?,?,?,?)""",
                (
                    str(uuid.uuid4()),
                    path,
                    asset_path,
                    now_ms(),
                    config.task_type,
                    config.collect_method,
                ),
            )
        return dict(self.db.execute("SELECT * FROM batches WHERE path=?", (path,)).fetchone())

    def get(self, batch_id):
        return dict(self.db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone())

    def rows(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM batches ORDER BY timestamp,path")]

    def update(self, batch_id, **values):
        allowed = {
            "state",
            "attempts",
            "retry_at",
            "error",
            "cleanup_pending",
            "pending",
            "files",
            "bytes",
        }
        if not values.keys() <= allowed:
            raise ValueError("Invalid batch update")
        with self.db:
            self.db.execute(
                "UPDATE batches SET " + ",".join(f"{k}=?" for k in values) + " WHERE id=?",
                (*values.values(), batch_id),
            )

    def acknowledge(self, batch_id, payload):
        with self.db:
            self.db.execute(
                "UPDATE batches SET pending=NULL WHERE id=? AND pending=?", (batch_id, payload)
            )

    def recover(self):
        with self.db:
            self.db.execute("""UPDATE batches SET state='queued',retry_at=0
                WHERE state IN ('copying','validating','uploading','interrupted','rejected','failed')""")

    def close(self):
        self.db.close()
