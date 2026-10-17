"""Bind a work directory to exactly one ingestion run mode."""

import json


def check_mode(config):
    marker = config.work_dir / "mode.json"
    existing = None
    if marker.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))["run_mode"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ValueError("工作目录 mode.json 无效，无法确认运行模式") from exc
    elif (config.work_dir / "batches.sqlite3").exists():
        # All databases created before run modes were added belong to the
        # original upload-and-report implementation.
        existing = "upload_and_report"
    if existing is not None and existing != config.run_mode:
        raise ValueError(
            f"工作目录绑定模式 {existing}，不能用于 {config.run_mode}；请指定独立 work_dir"
        )


def bind_mode(config):
    from .lifecycle import write_json

    check_mode(config)
    marker = config.work_dir / "mode.json"
    if not marker.exists():
        write_json(marker, dict(run_mode=config.run_mode))
