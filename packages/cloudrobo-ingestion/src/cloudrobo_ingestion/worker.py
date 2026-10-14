"""Killable, spawn-safe batch copy/upload worker."""

from __future__ import annotations

import threading
import time

from .filesystem import InterruptedBatch, RejectedBatch, checked_copy


def run_worker(config, batch, entries, connection, cleanup_only=False):
    stop = threading.Event()
    sending = threading.Lock()

    def send(event, **fields):
        try:
            with sending:
                connection.send(dict(event=event, **fields))
        except (OSError, EOFError):
            stop.set()

    def watch_parent():
        try:
            while connection.recv() != "stop":
                pass
        except (EOFError, OSError):
            # Parent died: do not leave blocked OBS SDK threads behind.
            import os

            os._exit(1)
        stop.set()

    threading.Thread(target=watch_parent, name="ingestion-parent-watch", daemon=True).start()
    checkpoint = config.work_dir / "checkpoints" / f"{batch['id']}.json"
    staging = config.work_dir / "staging" / batch["id"]
    client = None
    active = None
    try:
        from cloudrobo_core.sdk.obs_client import OBSClient

        if checkpoint.exists():
            client = OBSClient(bucket_name=config.bucket, timeout=5)
            if not client.abort_checkpoint_upload(str(checkpoint)):
                raise OSError("无法确认 OBS 分段上传已清理")
        if cleanup_only:
            send("cleaned")
            return
        checked_copy(config.source_dir / batch["path"], staging, entries, stop)
        if config.validation_enabled:
            send("phase", state="validating")
            from .validation import validate_lerobot

            validate_lerobot(staging, stop)
        if stop.is_set():
            raise InterruptedBatch()
        if entries and client is None:
            client = OBSClient(bucket_name=config.bucket, timeout=30)
        send("phase", state="uploading")
        for entry in entries:
            if stop.is_set():
                raise InterruptedBatch()
            active = entry.path
            send("file", path=active, state=1, transferred=0)
            last = [0.0]

            def progress(transferred, total, elapsed, path=entry.path, last=last):
                now = time.monotonic()
                if now - last[0] >= 0.2:
                    last[0] = now
                    send("file", path=path, state=1, transferred=transferred)
                if stop.is_set():
                    raise InterruptedBatch()

            key = (
                batch["asset_path"].split("/", 3)[3] if "/" in batch["asset_path"][6:] else ""
            ) + entry.path
            if not client.upload_file(
                str(staging / entry.path),
                key,
                progress_callback=progress,
                checkpoint_file=str(checkpoint),
            ):
                if stop.is_set():
                    raise InterruptedBatch()
                raise OSError(f"OBS 上传失败: {entry.path}")
            send("file", path=entry.path, state=2, transferred=entry.size)
            active = None
        send("result", state="done", error="")
    except InterruptedBatch:
        send("result", state="interrupted", error="上传已中断")
    except RejectedBatch as exc:
        send("result", state="rejected", error=str(exc))
    except Exception as exc:
        if active:
            send("file", path=active, state=3, transferred=0)
        # Never forward SDK exception bodies, which can contain credentials.
        send(
            "result",
            state="failed",
            error=f"{type(exc).__name__}: 回流失败" + (f" ({active})" if active else ""),
        )
    finally:
        if client:
            if checkpoint.exists():
                client.abort_checkpoint_upload(str(checkpoint))
            client.close()
        connection.close()
