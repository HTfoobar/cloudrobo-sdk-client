"""Separate real upload workspaces from mock workspaces."""

import json


def purpose(config):
    return "upload" if config.uploads else "mock"


def check_mode(config):
    marker = config.work_dir / "mode.json"
    existing = None
    if marker.exists():
        try:
            existing = json.loads(marker.read_text(encoding="utf-8"))["purpose"]
            if existing not in ("upload", "mock"):
                raise ValueError("Unknown workspace purpose")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ValueError("工作目录 mode.json 无效或版本不支持，请使用新的工作目录") from exc
    elif (config.work_dir / "batches.sqlite3").exists():
        raise ValueError("工作目录缺少用途标记，请使用新的工作目录")
    if existing is not None and existing != purpose(config):
        raise ValueError(
            f"工作目录绑定用途 {existing}，不能用于 {config.run_mode}；请指定独立 work_dir"
        )


def bind_mode(config):
    from .lifecycle import write_json

    check_mode(config)
    marker = config.work_dir / "mode.json"
    if not marker.exists():
        write_json(marker, dict(purpose=purpose(config)))
