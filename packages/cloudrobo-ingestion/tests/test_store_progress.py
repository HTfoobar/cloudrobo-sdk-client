import time

import pytest
from cloudrobo_ingestion.filesystem import FileEntry, manifest
from cloudrobo_ingestion.progress import BatchProgress, Reporter, message_class
from cloudrobo_ingestion.store import BatchStore


def test_batch_identity_and_times_survive_restart(config, batch_dir):
    store = BatchStore(config)
    b = store.add("批次001", config)
    entries = manifest(batch_dir)
    p = BatchProgress(b, entries, "r2c/local")
    first = message_class().FromString(p.serialize())
    store.update(b["id"], state="uploading")
    store.close()
    store = BatchStore(config)
    store.recover()
    b2 = store.add("批次001", config)
    second = message_class().FromString(BatchProgress(b2, entries, "r2c/local").serialize())
    assert first.timestamp == second.timestamp
    assert first.nodes[0].file_id == second.nodes[0].file_id
    assert first.nodes[0].enqueue_time == second.nodes[0].enqueue_time == first.timestamp
    assert b2["state"] == "queued"
    assert first.nodes[0].data_type == 4
    assert first.nodes[0].task_type == "超市取货"
    assert not any(
        "file" in r[0]
        for r in store.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    )
    store.close()


def test_done_survives_recover_outbox_ack_is_conditional(config):
    store = BatchStore(config)
    b = store.add("one", config)
    store.update(b["id"], state="done", pending=b"latest")
    store.recover()
    store.acknowledge(b["id"], b"old")
    assert store.get(b["id"])["pending"] == b"latest"
    store.acknowledge(b["id"], b"latest")
    assert store.get(b["id"])["pending"] is None
    assert store.get(b["id"])["state"] == "done"
    store.close()


def test_target_binding(config):
    from dataclasses import replace

    BatchStore(config).close()
    with pytest.raises(ValueError, match="绑定"):
        BatchStore(replace(config, obs_path="obs://other/"))


def test_progress_is_current_attempt_and_zero_when_stalled(config, batch_dir):
    store = BatchStore(config)
    b = store.add("批次001", config)
    p = BatchProgress(b, manifest(batch_dir), "r2c/local")
    path = next(iter(p.nodes))
    n = p.nodes[path]
    n.update(1, 0, at=0)
    n.update(1, 6, at=1)
    v = n.values(b, at=2)
    assert v["avg_bps"] == 3
    assert v["eta_ms"] > 0
    assert n.values(b, at=7)["avg_bps"] == 0
    p.update(path, 2, n.size)
    p.update(path, 1, 1)  # delayed SDK callback must not regress a completed file
    p.finished = True
    msg = message_class().FromString(p.serialize())
    assert msg.task_state == 1 and msg.nodes[0].progress == 100
    assert msg.nodes[0].avg_bps == 0
    retry = message_class().FromString(
        BatchProgress(b, manifest(batch_dir), "r2c/local").serialize()
    )
    assert retry.nodes[0].file_id == msg.nodes[0].file_id
    assert retry.nodes[0].progress == retry.nodes[0].start_time == 0
    store.close()


def test_reporter_merges_slots_and_retries_latest_snapshot(tmp_path):
    published = []
    online = [False]

    class Transport:
        def publish(self, topic, payload):
            if not online[0]:
                raise OSError("offline")
            published.append((topic, payload))

        def close(self):
            pass

    r = Reporter(
        tmp_path / "unused",
        "r2c/local/data_ingestion/upload_status",
        factory=lambda _: Transport(),
        interval=0.05,
    )
    r.set("batch", b"old", terminal=True)
    r.start()
    deadline = time.monotonic() + 1
    while not r.diagnostics()["error"] and time.monotonic() < deadline:
        time.sleep(0.01)
    for i in range(100):
        r.set("batch", str(i).encode(), terminal=True)
    assert r.diagnostics()["pending_batches"] == 1
    online[0] = True
    deadline = time.monotonic() + 3
    while not published and time.monotonic() < deadline:
        time.sleep(0.01)
    r.close()
    assert published == [("r2c/local/data_ingestion/upload_status", b"99")]
    assert r.acks.get_nowait() == ("batch", b"99")


def test_performance_5000_snapshot(config):
    store = BatchStore(config)
    b = store.add("large", config)
    p = BatchProgress(
        b, [FileEntry(f"data/file-{i:05}.parquet", 100000, 0, 0) for i in range(5000)], "r2c/local"
    )
    start = time.perf_counter()
    payload = p.serialize()
    elapsed = time.perf_counter() - start
    assert len(message_class().FromString(payload).nodes) == 5000
    assert elapsed < 1  # smoke budget; production p95/30-minute criteria are separate
    store.close()
