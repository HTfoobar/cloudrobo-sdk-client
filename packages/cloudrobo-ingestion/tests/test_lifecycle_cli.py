import json
import os

import pytest
from click.testing import CliRunner
from cloudrobo_ingestion.cli import ingestion
from cloudrobo_ingestion.lifecycle import InstanceLock, running, start, status, stop, write_json


def test_atomic_status_retries_busy_windows_reader(tmp_path, monkeypatch):
    original = os.replace
    attempts = []

    def replace(source, dest):
        attempts.append(1)
        if len(attempts) == 1:
            raise PermissionError("busy reader")
        original(source, dest)

    monkeypatch.setattr(os, "replace", replace)
    path = tmp_path / "status.json"
    write_json(path, {"state": "running"})
    assert json.loads(path.read_text()) == {"state": "running"}
    assert len(attempts) == 2


def test_single_instance_lock(tmp_path):
    with InstanceLock(tmp_path / "instance.lock"):
        assert running(tmp_path)
        with pytest.raises(RuntimeError, match="运行"):
            with InstanceLock(tmp_path / "instance.lock"):
                pass
    assert not running(tmp_path)


def test_cli_status_offline(tmp_path, monkeypatch):
    monkeypatch.setenv("CLOUDROBO_INGESTION_RUNTIME_DIR", str(tmp_path))
    result = CliRunner().invoke(ingestion, ["status", "--json"])
    assert result.exit_code == 0
    assert json.loads(result.output)["running"] is False
    result = CliRunner().invoke(ingestion, ["stop"])
    assert result.exit_code == 0


def test_actual_detached_daemon_lifecycle(config_file, tmp_path, monkeypatch):
    monkeypatch.setenv("CLOUDROBO_INGESTION_RUNTIME_DIR", str(tmp_path / "runtime"))
    # No data/OBS requests: test the real lock, startup handshake and IPC control.
    monkeypatch.setenv("HUAWEI_CLOUD_AK", "test-ak")
    monkeypatch.setenv("HUAWEI_CLOUD_SK", "test-sk")
    result = start(config_file)
    try:
        assert result["pid"] != os.getpid()
        assert status()["running"]
        with pytest.raises(RuntimeError, match="运行"):
            start(config_file)
        assert stop()["running"] is False
        assert not running()
    finally:
        if running():
            stop()


def test_start_failure_propagates_and_releases_lock(config_file, tmp_path, monkeypatch):
    monkeypatch.setenv("CLOUDROBO_INGESTION_RUNTIME_DIR", str(tmp_path / "runtime"))
    # Config succeeds but the runtime binding check in the detached child fails.
    import yaml
    from cloudrobo_ingestion.config import IngestionConfig
    from cloudrobo_ingestion.store import BatchStore

    c = IngestionConfig.load(config_file)
    c.work_dir.mkdir()
    BatchStore(c).close()
    data = yaml.safe_load(config_file.read_text())
    data["obs_path"] = "obs://different/target/"
    config_file.write_text(yaml.safe_dump(data))
    monkeypatch.setenv("HUAWEI_CLOUD_AK", "test-ak")
    monkeypatch.setenv("HUAWEI_CLOUD_SK", "test-sk")
    with pytest.raises(RuntimeError, match="绑定"):
        start(config_file)
    assert not running()
