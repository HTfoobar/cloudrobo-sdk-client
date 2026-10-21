import builtins
import json
from dataclasses import replace

import pytest
import yaml
from click.testing import CliRunner
from cloudrobo_ingestion.cli import ingestion
from cloudrobo_ingestion.config import IngestionConfig
from cloudrobo_ingestion.store import BatchStore
from cloudrobo_ingestion.workspace import bind_mode, check_mode


def update(path, **values):
    data = yaml.safe_load(path.read_text())
    data.update(values)
    path.write_text(yaml.safe_dump(data))


@pytest.mark.parametrize("mode", ["upload_and_report", "upload_only"])
@pytest.mark.parametrize("value", [None, True, False, "100", 1.5, 0, -1])
def test_invalid_batch_file_limit(config_file, mode, value):
    update(config_file, run_mode=mode, max_files_per_batch=value)
    with pytest.raises(ValueError, match="max_files_per_batch"):
        IngestionConfig.load(config_file)


def test_batch_file_limit_priority_and_effective_config(config_file):
    assert IngestionConfig.load(config_file).max_files_per_batch == 100
    update(config_file, max_files_per_batch=5001)
    assert (
        IngestionConfig.load(config_file, dict(max_files_per_batch=None)).max_files_per_batch
        == 5001
    )
    c = IngestionConfig.load(config_file, dict(max_files_per_batch=200))
    assert c.max_files_per_batch == 200
    assert IngestionConfig.from_mapping(c.to_mapping(), config_file.parent) == c
    assert yaml.safe_load(config_file.read_text())["max_files_per_batch"] == 5001


@pytest.mark.parametrize("limit", ["1", "5001", "0", "-1"])
def test_cli_batch_file_limit(config_file, monkeypatch, limit):
    from cloudrobo_ingestion import lifecycle

    received = []

    def start(path, overrides=None):
        received.append(IngestionConfig.load(path, overrides))
        return dict(pid=123, work_dir="work", run_mode="upload_and_report")

    monkeypatch.setattr(lifecycle, "start", start)
    result = CliRunner().invoke(
        ingestion, ["start", "--config", str(config_file), "--max-files-per-batch", limit]
    )
    if int(limit) > 0:
        assert result.exit_code == 0, result.output
        assert received[0].max_files_per_batch == int(limit)
    else:
        assert result.exit_code != 0
        assert not received


def test_legacy_config_defaults_to_combined(config_file):
    c = IngestionConfig.load(config_file)
    assert c.run_mode == "upload_and_report" and c.uploads and c.reports


@pytest.mark.parametrize("value", ["upload", "client", "", None, 12])
def test_unknown_run_mode(config_file, value):
    update(config_file, run_mode=value)
    with pytest.raises(ValueError, match="run_mode"):
        IngestionConfig.load(config_file)


def test_upload_only_ignores_unused_report_settings(config_file, monkeypatch):
    data = yaml.safe_load(config_file.read_text())
    del data["task_type"], data["collect_method"]
    data.update(run_mode="upload_only", zenoh="intentionally invalid", mock="unused")
    config_file.write_text(yaml.safe_dump(data))
    original = builtins.__import__

    def guard(name, *args, **kwargs):
        if name.startswith(("cloudrobo_r2c", "zenoh", "google.protobuf")):
            raise AssertionError("upload_only imported report dependency")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guard)
    c = IngestionConfig.load(config_file, dict(zenoh_mode="client", mock_file_count=3))
    assert c.uploads and not c.reports
    assert c.task_type == c.collect_method == ""
    assert c.zenoh == {}
    assert IngestionConfig.from_mapping(c.to_mapping(), config_file.parent) == c


@pytest.mark.parametrize("value", [None, 0, "", "a" * 65])
def test_upload_only_validates_metadata_when_provided(config_file, value):
    update(config_file, run_mode="upload_only", task_type=value)
    with pytest.raises(ValueError, match="task_type"):
        IngestionConfig.load(config_file)


def test_mock_needs_neither_source_nor_obs(config_file):
    data = yaml.safe_load(config_file.read_text())
    del data["source_dir"], data["obs_path"]
    data["run_mode"] = "mock_report"
    config_file.write_text(yaml.safe_dump(data))
    c = IngestionConfig.load(config_file)
    assert c.source_dir is None and not c.uploads and c.reports
    assert c.obs_path == "obs://mock-bucket/ingestion/"
    assert c.mock["file_count"] == 10


@pytest.mark.parametrize(
    "field,value",
    [
        ("file_count", 0),
        ("file_count", 5001),
        ("file_count", True),
        ("file_size_bytes", 0),
        ("file_size_bytes", 1.5),
        ("file_size_bytes", 2**63),
        ("upload_bps", 0),
        ("upload_bps", float("inf")),
        ("upload_bps", True),
        ("upload_bps", 10**400),
        ("batch_interval_seconds", -1),
        ("batch_interval_seconds", float("nan")),
    ],
)
def test_invalid_mock_parameters(config_file, field, value):
    update(config_file, run_mode="mock_report", mock={field: value})
    with pytest.raises(ValueError, match=field):
        IngestionConfig.load(config_file)


def test_mock_rejects_validation_and_cli_can_disable(config_file):
    update(config_file, run_mode="mock_report", validation={"enabled": True})
    with pytest.raises(ValueError, match="mock_report"):
        IngestionConfig.load(config_file)
    c = IngestionConfig.load(
        config_file, dict(validation_enabled=False, mock_batch_interval_seconds=0)
    )
    assert not c.validation_enabled and c.mock["batch_interval_seconds"] == 0


def test_cli_merge_replaces_endpoints_and_preserves_advanced_options(config_file):
    update(
        config_file,
        zenoh=dict(
            mode="peer",
            endpoints=["tcp/old:7447"],
            connect=dict(endpoints=["tcp/actual-old:7447"], timeout_ms=1234, exit_on_failure=True),
            tls=dict(enabled=False, root_ca_certificate="/path/to/ca.pem"),
            publisher_priority="BACKGROUND",
        ),
    )
    before = config_file.read_bytes()
    c = IngestionConfig.load(
        config_file,
        dict(
            zenoh_mode="client",
            zenoh_endpoint=("tcp/first:7447", "tcp/second:7447"),
            zenoh_project_id="test-project",
            zenoh_device_id="test-device",
            zenoh_client_id="test-client",
            task_type="new-task",
            collect_method="new-method",
            validation_enabled=False,
        ),
    )
    assert c.zenoh["mode"] == "client"
    assert (
        c.zenoh["endpoints"]
        == c.zenoh["connect"]["endpoints"]
        == ["tcp/first:7447", "tcp/second:7447"]
    )
    assert c.zenoh["connect"]["timeout_ms"] == 1234
    assert c.zenoh["tls"]["root_ca_certificate"] == "/path/to/ca.pem"
    assert c.zenoh["publisher_priority"] == "BACKGROUND"
    assert c.task_type == "new-task" and c.collect_method == "new-method"
    assert c.topic == "test-project/test-device/data_ingestion/upload_status"
    assert config_file.read_bytes() == before
    untouched = IngestionConfig.load(config_file, dict(zenoh_mode=None, zenoh_endpoint=()))
    assert untouched.zenoh["mode"] == "peer"
    assert untouched.zenoh["connect"]["endpoints"] == ["tcp/actual-old:7447"]


def test_cli_relative_paths_use_cwd_not_yaml_directory(config_file, tmp_path, monkeypatch):
    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "cli-source").mkdir(parents=True)
    monkeypatch.chdir(elsewhere)
    c = IngestionConfig.load(config_file, dict(source_dir="cli-source", work_dir="cli-work"))
    assert c.source_dir == elsewhere / "cli-source" and c.work_dir == elsewhere / "cli-work"
    plain = IngestionConfig.load(config_file)
    assert plain.source_dir == config_file.parent / "source"
    assert plain.work_dir == config_file.parent / "work"


def test_cli_passes_only_explicit_overrides(config_file, monkeypatch):
    from cloudrobo_ingestion import lifecycle

    received = []

    def start(path, overrides=None):
        received.append((path, overrides))
        return dict(pid=123, work_dir="mock-work", run_mode="mock_report")

    monkeypatch.setattr(lifecycle, "start", start)
    result = CliRunner().invoke(
        ingestion,
        [
            "start",
            "--config",
            str(config_file),
            "--run-mode",
            "mock_report",
            "--no-validate-lerobot",
            "--mock-batch-interval-seconds",
            "0",
            "--zenoh-mode",
            "peer",
            "--zenoh-endpoint",
            "tcp/a:7447",
            "--zenoh-endpoint",
            "tcp/b:7447",
        ],
    )
    assert result.exit_code == 0, result.output
    overrides = received[0][1]
    assert overrides["validation_enabled"] is False
    assert overrides["mock_batch_interval_seconds"] == 0
    assert overrides["run_mode"] == "mock_report"
    assert overrides["zenoh_mode"] == "peer"
    assert overrides["zenoh_endpoint"] == ("tcp/a:7447", "tcp/b:7447")
    assert overrides["source_dir"] is None and overrides["task_type"] is None
    assert "mock_report" in result.output


def test_cli_absent_flags_preserve_config(config_file, monkeypatch):
    from cloudrobo_ingestion import lifecycle

    captured = []

    def start(path, overrides=None):
        captured.append(overrides)
        return dict(pid=123, work_dir="existing-work", run_mode="upload_and_report")

    monkeypatch.setattr(lifecycle, "start", start)
    result = CliRunner().invoke(ingestion, ["start", "--config", str(config_file)])
    assert result.exit_code == 0, result.output
    assert captured[0]["validation_enabled"] is None
    assert captured[0]["run_mode"] is None
    assert captured[0]["max_files_per_batch"] is None
    assert captured[0]["zenoh_endpoint"] == ()


def test_cli_all_mock_overrides_roundtrip(config_file):
    c = IngestionConfig.load(
        config_file,
        dict(
            run_mode="mock_report",
            obs_path="obs://virtual/new/",
            task_type="custom-task",
            collect_method="custom-method",
            mock_file_count=2,
            mock_file_size_bytes=1024,
            mock_upload_bps=128.5,
            mock_batch_interval_seconds=0,
        ),
    )
    assert c.mock == dict(
        file_count=2, file_size_bytes=1024, upload_bps=128.5, batch_interval_seconds=0
    )
    assert c.obs_path == "obs://virtual/new/" and c.source_dir is None
    assert IngestionConfig.from_mapping(c.to_mapping(), config_file.parent) == c


def test_work_dir_binding_and_legacy_default(config):
    store = BatchStore(config)
    b = store.add("batch", config)
    store.update(b["id"], state="done", pending=b"original-pending")
    store.close()
    for mode in ("upload_only", "mock_report"):
        with pytest.raises(ValueError, match="独立 work_dir"):
            check_mode(replace(config, run_mode=mode))
    (config.work_dir / "mode.json").unlink()  # simulate a pre-mode installation
    check_mode(config)
    with pytest.raises(ValueError, match="upload_and_report"):
        check_mode(replace(config, run_mode="upload_only"))
    recovered = BatchStore(config)
    assert recovered.get(b["id"])["pending"] == b"original-pending"
    assert recovered.get(b["id"])["state"] == "done"
    recovered.close()


def test_two_real_modes_keep_history_independent(config):
    first = BatchStore(config)
    a = first.add("same-source-batch", config)
    first.update(a["id"], state="done", pending=b"real-final")
    other_dir = config.work_dir.parent / "upload-only-work"
    other_dir.mkdir()
    pure = replace(
        config, work_dir=other_dir, run_mode="upload_only", task_type="", collect_method=""
    )
    second = BatchStore(pure)
    b = second.add("same-source-batch", pure)
    assert a["id"] != b["id"]
    assert b["state"] == "queued" and b["pending"] is None
    second.update(b["id"], state="done")
    second.close()
    assert first.get(a["id"])["pending"] == b"real-final"
    first.close()


def test_mock_binding_does_not_create_database(config):
    mock = replace(config, run_mode="mock_report", source_dir=None)
    bind_mode(mock)
    assert json.loads((config.work_dir / "mode.json").read_text())["run_mode"] == "mock_report"
    assert not (config.work_dir / "batches.sqlite3").exists()
    with pytest.raises(ValueError, match="绑定模式"):
        check_mode(config)
