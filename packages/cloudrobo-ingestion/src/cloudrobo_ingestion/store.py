"""Single-writer, batch-only durable recovery state."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid

logger = logging.getLogger(__name__)


def now_ms():
    return time.time_ns() // 1_000_000


class BatchStore:
    def __init__(self, config):
        from .workspace import bind_mode

        if not config.uploads:
            raise ValueError("Mock 模式不使用真实批次数据库")
        bind_mode(config)
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
                retry_at REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '', error_info TEXT,
                cleanup_error_info TEXT,
                cleanup_pending INTEGER NOT NULL DEFAULT 0, pending BLOB,
                report_enabled INTEGER NOT NULL DEFAULT 0, final_snapshot TEXT,
                files INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0
            );
        """)
        source = str(config.source_dir)
        row = self.db.execute("SELECT value FROM settings WHERE key='source_dir'").fetchone()
        if row and row[0] != source:
            self.db.close()
            raise ValueError("工作目录绑定的 source_dir 不同，请使用新的工作目录")
        self.db.execute("INSERT OR IGNORE INTO settings VALUES ('source_dir',?)", (source,))
        self.db.commit()
        self.configure_batches(config)

    @staticmethod
    def target(config, path):
        return config.obs_path + (path.rstrip("/") + "/" if path != "." else "")

    def configure_batches(self, config):
        """Apply startup policy without touching completed upload history."""
        changed = 0
        with self.db:
            for batch in self.db.execute("SELECT * FROM batches WHERE state!='done'").fetchall():
                target = self.target(config, batch["path"])
                if target != batch["asset_path"]:
                    self.db.execute(
                        "UPDATE batches SET asset_path=?,pending=NULL WHERE id=?",
                        (target, batch["id"]),
                    )
                    changed += 1
                    logger.info(
                        "OBS 目标变更 batch_id=%s old=%s new=%s；未完成批次将整批重试",
                        batch["id"],
                        batch["asset_path"],
                        target,
                    )
                if config.reports and not batch["report_enabled"]:
                    self.db.execute(
                        """UPDATE batches SET report_enabled=1,
                        task_type=?,collect_method=? WHERE id=?""",
                        (
                            batch["task_type"] or config.task_type,
                            batch["collect_method"] or config.collect_method,
                            batch["id"],
                        ),
                    )
        if changed:
            logger.info("OBS 目标已调整，受影响未完成批次：%s", changed)

    def add(self, path, config):
        asset_path = self.target(config, path)
        with self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO batches
                (id,path,asset_path,timestamp,task_type,collect_method,report_enabled)
                VALUES (?,?,?,?,?,?,?)""",
                (
                    str(uuid.uuid4()),
                    path,
                    asset_path,
                    now_ms(),
                    config.task_type,
                    config.collect_method,
                    int(config.reports),
                ),
            )
        return self._decode(
            self.db.execute("SELECT * FROM batches WHERE path=?", (path,)).fetchone()
        )

    def get(self, batch_id):
        return self._decode(
            self.db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        )

    @staticmethod
    def _decode(row):
        result = dict(row)
        for key in ("error_info", "cleanup_error_info", "final_snapshot"):
            result[key] = json.loads(result[key]) if result[key] else None
        return result

    def rows(self):
        return [
            self._decode(r)
            for r in self.db.execute("SELECT * FROM batches ORDER BY timestamp,path")
        ]

    def finals_to_prepare(self):
        return [
            self._decode(r)
            for r in self.db.execute(
                """SELECT * FROM batches WHERE state='done'
                AND final_snapshot IS NOT NULL AND pending IS NULL"""
            )
        ]

    def update(self, batch_id, **values):
        allowed = {
            "state",
            "attempts",
            "retry_at",
            "error",
            "error_info",
            "cleanup_error_info",
            "cleanup_pending",
            "pending",
            "files",
            "bytes",
            "final_snapshot",
        }
        if not values.keys() <= allowed:
            raise ValueError("Invalid batch update")
        for key in ("error_info", "cleanup_error_info", "final_snapshot"):
            if values.get(key) is not None:
                values[key] = json.dumps(values[key], ensure_ascii=False)
        with self.db:
            self.db.execute(
                "UPDATE batches SET " + ",".join(f"{k}=?" for k in values) + " WHERE id=?",
                (*values.values(), batch_id),
            )

    def acknowledge(self, batch_id, payload):
        with self.db:
            self.db.execute(
                "UPDATE batches SET pending=NULL,final_snapshot=NULL WHERE id=? AND pending=?",
                (batch_id, payload),
            )

    def recover(self):
        with self.db:
            self.db.execute("""UPDATE batches SET state='queued',retry_at=0
                WHERE state IN ('copying','validating','uploading','interrupted','rejected','failed')""")

    def close(self):
        self.db.close()
