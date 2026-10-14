"""Validated configuration; validation libraries are imported only on request."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import yaml


@dataclass(frozen=True)
class IngestionConfig:
    source_dir: Path
    work_dir: Path
    obs_path: str
    task_type: str
    collect_method: str
    validation_enabled: bool = False
    zenoh: dict = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> IngestionConfig:
        path = Path(path).resolve()
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("配置必须是 YAML 对象")
        for key in ("source_dir", "work_dir", "obs_path", "task_type", "collect_method"):
            value = data.get(key)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{key} 必须是非空字符串")
        for key in ("task_type", "collect_method"):
            if not 1 <= len(data[key]) <= 64:
                raise ValueError(f"{key} 长度必须为 1～64 个字符")

        def resolve(value):
            p = Path(value).expanduser()
            p = path.parent / p if not p.is_absolute() else p
            from .filesystem import is_link

            if p.exists() and is_link(p):
                raise ValueError("源目录和工作目录不允许链接或 junction")
            return p.resolve()

        source, work = resolve(data["source_dir"]), resolve(data["work_dir"])
        if not source.is_dir():
            raise ValueError("source_dir 必须是现有目录")
        if source == work or source in work.parents or work in source.parents:
            raise ValueError("源目录和工作目录不能相同或互相包含")
        uri = urlsplit(data["obs_path"])
        if (
            uri.scheme != "obs"
            or not uri.netloc
            or uri.query
            or uri.fragment
            or "@" in uri.netloc
            or ":" in uri.netloc
            or any(p in (".", "..") for p in uri.path.split("/"))
        ):
            raise ValueError("obs_path 必须是 obs://bucket/prefix/ 路径")
        validation = data.get("validation", {})
        if not isinstance(validation, dict):
            raise ValueError("validation 必须是对象")
        enabled = validation.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("validation.enabled 必须是布尔值")
        if validation.get("format", "lerobot_v3") != "lerobot_v3":
            raise ValueError("只支持 validation.format=lerobot_v3")
        if enabled:
            from .validation import require_dependencies

            require_dependencies()
        z = data.get("zenoh", {})
        if not isinstance(z, dict):
            raise ValueError("zenoh 必须是对象")
        z = (
            dict(
                mode="client",
                endpoint_role="connect",
                endpoints=["tcp/127.0.0.1:7447"],
                project_id="r2c",
                device_id="local",
                client_id="cloudrobo-ingestion",
            )
            | z
        )
        for key in ("project_id", "device_id", "client_id"):
            if not isinstance(z[key], str) or not z[key] or any(c in z[key] for c in "/*?#$"):
                raise ValueError(f"zenoh.{key} 必须是单段身份字符串")
        # Full R2C option parsing is performed from a dedicated YAML at startup.
        if z["mode"] not in ("client", "peer") or z["endpoint_role"] != "connect":
            raise ValueError("zenoh mode 无效，endpoint_role 必须为 connect")
        if (
            not isinstance(z["endpoints"], list)
            or not z["endpoints"]
            or any(not isinstance(e, str) or not e for e in z["endpoints"])
        ):
            raise ValueError("zenoh.endpoints 必须是非空字符串列表")
        return cls(
            source,
            work,
            f"obs://{uri.netloc}/{uri.path.strip('/')}".rstrip("/") + "/",
            data["task_type"],
            data["collect_method"],
            enabled,
            z,
        )

    @property
    def bucket(self) -> str:
        return urlsplit(self.obs_path).netloc

    @property
    def prefix(self) -> str:
        return urlsplit(self.obs_path).path.lstrip("/")

    @property
    def topic(self) -> str:
        return f"{self.zenoh['project_id']}/{self.zenoh['device_id']}/data_ingestion/upload_status"
