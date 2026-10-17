import builtins
from dataclasses import replace

from cloudrobo_ingestion.mock_runtime import MockRunner
from cloudrobo_ingestion.progress import Reporter, message_class


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


def runner(config, clock):
    c = replace(
        config,
        run_mode="mock_report",
        source_dir=None,
        mock=dict(file_count=2, file_size_bytes=100, upload_bps=100, batch_interval_seconds=2),
    )
    r = Reporter("unused", c.topic)
    return MockRunner(c, r, c.work_dir / "status.json", clock=clock), r


def test_mock_progress_eta_completion_and_cycle(config, monkeypatch):
    original = builtins.__import__

    def guard(name, *args, **kwargs):
        if name == "obs" or name.startswith("obs.") or name.endswith("obs_client"):
            raise AssertionError("Mock accessed OBS")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guard)
    clock = Clock()
    m, r = runner(config, clock)
    m.tick()  # enqueue all files
    first_id = m.progress.batch["id"]
    queued = message_class().FromString(m.progress.serialize())
    assert len(queued.nodes) == 2 and all(n.result_state == 0 for n in queued.nodes)
    assert queued.source == "r2c/local" and queued.nodes[0].data_type == 0
    m.tick()  # start first file
    clock.now = 0.5
    m.tick()
    p = message_class().FromString(m.progress.serialize())
    assert p.nodes[0].progress == 50 and p.nodes[0].avg_bps == 100 and p.nodes[0].eta_ms == 500
    assert p.nodes[1].result_state == 0
    clock.now = 1
    m.tick()  # finish first
    m.tick()  # start second
    clock.now = 2
    m.tick()
    assert m.phase == "awaiting_publish"
    final = message_class().FromString(m.final_payload)
    assert final.task_state == 1 and all(
        n.progress == 100 and n.result_state == 2 for n in final.nodes
    )
    # An unrelated/stale progress ack cannot start another batch.
    r.acks.put((first_id, queued.SerializeToString()))
    m.tick()
    assert m.phase == "awaiting_publish"
    r.acks.put((first_id, m.final_payload))
    m.tick()
    assert m.completed == 1 and m.phase == "waiting"
    clock.now = 3.9
    m.tick()
    assert m.progress.batch["id"] == first_id
    clock.now = 4
    m.tick()
    second = message_class().FromString(m.progress.serialize())
    assert m.progress.batch["id"] != first_id
    assert second.nodes[0].file_id != queued.nodes[0].file_id
    assert second.asset_path != queued.asset_path
    assert not (config.work_dir / "batches.sqlite3").exists()
    assert not (config.work_dir / "staging").exists()
    assert not (config.work_dir / "checkpoints").exists()


def test_mock_offline_remains_one_batch_and_stops(config):
    clock = Clock()
    m, r = runner(config, clock)
    m.tick()
    m.tick()
    clock.now = 1
    m.tick()
    m.tick()
    clock.now = 2
    m.tick()
    batch_id = m.progress.batch["id"]
    for i in range(1000):
        clock.now = 3 + i
        m.tick()
    assert m.phase == "awaiting_publish" and m.completed == 0
    assert m.progress.batch["id"] == batch_id
    assert r.diagnostics()["pending_batches"] == 1
    m.stop_event.set()
    r.acks.put((batch_id, m.final_payload))
    m.tick()
    assert m.progress.batch["id"] == batch_id
    assert m.snapshot()["run_mode"] == "mock_report" and m.snapshot()["mock"] is True


def test_large_legal_mock_file_does_not_overflow_proto_eta(config):
    c = replace(
        config,
        run_mode="mock_report",
        source_dir=None,
        mock=dict(file_count=1, file_size_bytes=2**63 - 1, upload_bps=1, batch_interval_seconds=0),
    )
    clock = Clock()
    m = MockRunner(c, Reporter("unused", c.topic), c.work_dir / "status.json", clock=clock)
    m.tick()
    m.tick()
    clock.now = 1
    m.tick()
    msg = message_class().FromString(m.progress.serialize())
    assert msg.nodes[0].size == 2**63 - 1
    assert msg.nodes[0].eta_ms == 0
