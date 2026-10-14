import threading
import time
from pathlib import Path

import pytest
from cloudrobo_ingestion.daemon import Supervisor
from cloudrobo_ingestion.filesystem import checked_copy, manifest
from cloudrobo_ingestion.progress import Reporter, message_class
from cloudrobo_ingestion.store import BatchStore
from cloudrobo_ingestion.worker import run_worker


class Connection:
    def __init__(self):
        self.messages = []
        self.closed = threading.Event()

    def send(self, message):
        self.messages.append(message)

    def recv(self):
        self.closed.wait()
        return "stop"

    def close(self):
        self.closed.set()


@pytest.mark.parametrize("success", [True, False])
def test_worker_obs_and_progress(config, batch_dir, monkeypatch, success):
    from cloudrobo_core.sdk import obs_client

    calls = []

    class OBS:
        def __init__(self, **kwargs):
            pass

        def upload_file(self, path, key, **kwargs):
            assert Path(path).is_relative_to(config.work_dir)
            kwargs["progress_callback"](4, 13, 0.1)
            calls.append(key)
            return success

        def close(self):
            pass

    monkeypatch.setattr(obs_client, "OBSClient", OBS)
    store = BatchStore(config)
    batch = store.add("批次001", config)
    conn = Connection()
    run_worker(config, batch, manifest(batch_dir), conn)
    assert calls == ["prefix/批次001/data/file-000.parquet"]
    result = [e for e in conn.messages if e["event"] == "result"][0]
    assert result["state"] == ("done" if success else "failed")
    assert any(e["event"] == "file" and e["state"] == 1 for e in conn.messages)
    assert (batch_dir / "data/file-000.parquet").read_bytes() == b"customer-data"
    store.close()


def successful_child(config, batch, entries, connection, cleanup_only):
    if cleanup_only:
        connection.send(dict(event="cleaned"))
    else:
        checked_copy(
            config.source_dir / batch["path"],
            config.work_dir / "staging" / batch["id"],
            entries,
            threading.Event(),
        )
        for e in entries:
            connection.send(dict(event="file", path=e.path, state=2, transferred=e.size))
        connection.send(dict(event="result", state="done", error=""))
    connection.close()


def failed_child(config, batch, entries, connection, cleanup_only):
    (config.work_dir / "staging" / batch["id"]).mkdir()
    connection.send(dict(event="result", state="failed", error="mock upload failed"))
    connection.close()


def blocked_child(config, batch, entries, connection, cleanup_only):
    (config.work_dir / "staging" / batch["id"]).mkdir()
    time.sleep(60)


def drain_until_done(s):
    deadline = time.monotonic() + 10
    while s.process is not None and time.monotonic() < deadline:
        s.drain()
        time.sleep(0.02)
    assert s.process is None


def test_supervisor_success_cleanup_and_skip_restart(config, batch_dir):
    reporter = Reporter("unused", config.topic)
    s = Supervisor(
        config, reporter, config.work_dir / "status.json", worker_target=successful_child
    )
    s.initialize()
    s.scan()
    s.launch_next()
    drain_until_done(s)
    b = s.store.rows()[0]
    assert b["state"] == "done" and not b["cleanup_pending"]
    assert not (config.work_dir / "staging" / b["id"]).exists()
    assert message_class().FromString(b["pending"]).task_state == 1
    s.store.close()
    restarted = Supervisor(
        config, reporter, config.work_dir / "status.json", worker_target=successful_child
    )
    restarted.initialize()
    restarted.scan()
    restarted.launch_next()
    assert restarted.process is None
    assert restarted.store.rows()[0]["id"] == b["id"]
    restarted.store.close()


def test_supervisor_failure_retries_after_recopy(config, batch_dir):
    r = Reporter("unused", config.topic)
    s = Supervisor(config, r, config.work_dir / "status.json", worker_target=failed_child)
    s.scan()
    s.launch_next()
    drain_until_done(s)
    b = s.store.rows()[0]
    assert b["state"] == "failed" and b["retry_at"] > time.time()
    assert not (config.work_dir / "staging" / b["id"]).exists()
    s.launch_next()
    assert s.process is None
    s.store.update(b["id"], retry_at=0)
    s.worker_target = successful_child
    s.launch_next()
    drain_until_done(s)
    new = s.store.rows()[0]
    assert new["state"] == "done" and new["attempts"] == 2
    assert new["id"] == b["id"] and new["timestamp"] == b["timestamp"]
    s.store.close()


def test_recovery_cleans_interrupted_staging(config, batch_dir):
    store = BatchStore(config)
    b = store.add("批次001", config)
    stage = config.work_dir / "staging" / b["id"]
    stage.mkdir(parents=True)
    (stage / "leftover").write_bytes(b"partial")
    store.update(b["id"], state="uploading", cleanup_pending=1)
    store.close()
    s = Supervisor(config, Reporter("unused", config.topic), config.work_dir / "status.json")
    s.initialize()
    assert not stage.exists()
    assert s.store.get(b["id"])["state"] == "queued"
    s.store.close()


def test_stop_terminates_blocked_upload(config, batch_dir):
    r = Reporter("unused", config.topic, factory=lambda _: None)
    s = Supervisor(config, r, config.work_dir / "status.json", worker_target=blocked_child)
    s.scan()
    s.launch_next()
    r.start()
    s.stop_event.set()
    start = time.monotonic()
    s.run()
    assert time.monotonic() - start < 9
    assert s.process is None
    store = BatchStore(config)
    b = store.rows()[0]
    assert b["state"] == "interrupted"
    assert not (config.work_dir / "staging" / b["id"]).exists()
    store.close()
