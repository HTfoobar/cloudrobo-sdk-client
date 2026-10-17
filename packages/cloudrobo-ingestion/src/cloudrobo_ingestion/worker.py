"""Killable, spawn-safe batch copy/upload worker."""

from __future__ import annotations

import json
import threading
import time

from .filesystem import InterruptedBatch, RejectedBatch, checked_copy


def run_worker(config, batch, entries, connection, cleanup_only=False):
    from cloudrobo_core.sdk.obs_diagnostics import credential_values, exception_details, redact

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
    stage = "initializing"
    upload_error = None

    def log(level, message, **details):
        context = dict(
            batch_id=batch["id"],
            batch_path=batch["path"],
            stage=stage,
            file=active,
            source_path=str(config.source_dir / batch["path"]),
            staging=str(staging),
            checkpoint=str(checkpoint),
            asset_path=batch["asset_path"],
            **details,
        )
        # Only the parent writes/rotates the file, including on Windows spawn.
        send(
            "log",
            level=level,
            message=redact(
                message + " " + json.dumps(context, ensure_ascii=False),
                credential_values(client),
                limit=16000,
            ),
        )

    try:
        log("INFO", "Starting batch worker", cleanup_only=cleanup_only)
        from cloudrobo_core.sdk.obs_client import OBSClient

        if checkpoint.exists():
            stage = "checkpoint_cleanup"
            client = OBSClient(bucket_name=config.bucket, timeout=5)
            if not client.abort_checkpoint_upload(str(checkpoint)):
                raise OSError("无法确认 OBS 分段上传已清理")
        if cleanup_only:
            send("cleaned")
            return
        stage = "copying"
        log("INFO", "Copying batch", files=len(entries))
        checked_copy(config.source_dir / batch["path"], staging, entries, stop)
        if config.validation_enabled:
            stage = "validating"
            send("phase", state="validating")
            from .validation import validate_lerobot

            validate_lerobot(staging, stop)
        if stop.is_set():
            raise InterruptedBatch()
        if entries and client is None:
            stage = "obs_initializing"
            client = OBSClient(bucket_name=config.bucket, timeout=30)
        stage = "uploading"
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
            log(
                "INFO",
                "Uploading file",
                local_path=str(staging / entry.path),
                object_key=key,
                size=entry.size,
                endpoint=getattr(client, "endpoint", ""),
            )
            if not client.upload_file(
                str(staging / entry.path),
                key,
                progress_callback=progress,
                checkpoint_file=str(checkpoint),
            ):
                if stop.is_set():
                    raise InterruptedBatch()
                upload_error = getattr(client, "last_upload_error", None)
                raise OSError(f"OBS 上传失败: {entry.path}")
            send("file", path=entry.path, state=2, transferred=entry.size)
            log("INFO", "File uploaded")
            active = None
        log("INFO", "Batch upload completed")
        send("result", state="done", error="")
    except InterruptedBatch:
        send("result", state="interrupted", error="上传已中断")
    except RejectedBatch as exc:
        log(
            "WARNING",
            "Batch rejected",
            diagnostic=exception_details(exc, credential_values(client)),
        )
        send("result", state="rejected", error=str(exc))
    except Exception as exc:
        if active:
            send("file", path=active, state=3, transferred=0)
        diagnostic = upload_error or exception_details(exc, credential_values(client))
        log("ERROR", "Batch failed", diagnostic=diagnostic)
        summary = "; ".join(
            f"{key}={diagnostic[key]}"
            for key in (
                "http_status",
                "error_code",
                "request_id",
                "exception_type",
                "errno",
                "winerror",
                "message",
            )
            if diagnostic.get(key) not in (None, "")
        )
        send(
            "result",
            state="failed",
            error=redact(
                f"{type(exc).__name__}: 回流失败 [stage={stage}]"
                + (f" ({active})" if active else "")
                + (f": {summary}" if summary else ""),
                credential_values(client),
                limit=2000,
            ),
        )
    finally:
        if client:
            stage = "cleanup"
            try:
                if checkpoint.exists() and not client.abort_checkpoint_upload(str(checkpoint)):
                    log("WARNING", "OBS checkpoint cleanup not confirmed")
            except Exception as exc:
                log(
                    "ERROR",
                    "OBS checkpoint cleanup failed",
                    diagnostic=exception_details(exc, credential_values(client)),
                )
            try:
                client.close()
            except Exception as exc:
                log(
                    "ERROR",
                    "OBS client close failed",
                    diagnostic=exception_details(exc, credential_values(client)),
                )
        connection.close()
