import socket
import threading

import pytest
import yaml
from cloudrobo_ingestion.progress import Reporter, message_class


def test_real_transport_protobuf_to_local_listener(tmp_path):
    zenoh = pytest.importorskip("zenoh")
    # A local listening Zenoh peer exercises the same client -> router wire path
    # without requiring a separately installed zenohd executable.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    cfg = zenoh.Config()
    cfg.insert_json5("listen/endpoints", json_string([f"tcp/127.0.0.1:{port}"]))
    cfg.insert_json5("scouting/multicast/enabled", "false")
    session = zenoh.open(cfg)
    received = []
    event = threading.Event()
    topic = "r2c/local/data_ingestion/upload_status"

    def on_sample(sample):
        received.append(message_class().FromString(sample.payload.to_bytes()))
        event.set()

    subscriber = session.declare_subscriber(topic, on_sample)
    path = tmp_path / "zenoh.yaml"
    path.write_text(
        yaml.safe_dump(
            dict(
                project_id="r2c",
                device_id="local",
                client_id="test-ingestion",
                mode="client",
                endpoint_role="connect",
                endpoints=[f"tcp/127.0.0.1:{port}"],
            )
        )
    )
    reporter = Reporter(path, topic, interval=0.1)
    reporter.start()
    payload = message_class()(
        source="r2c/local", timestamp=123, asset_path="obs://bucket/batch/", task_state=1
    )
    payload.nodes.add(
        file_id="stable-id",
        task_name="a.mp4",
        size=123,
        progress=100,
        result_state=2,
        task_type="超市取货",
        collect_method="遥操接管",
    )
    try:
        # Subscription discovery may race the first send; use periodic snapshots.
        reporter.set("one", payload.SerializeToString())
        assert event.wait(5)
        assert received[-1].nodes[0].task_name == "a.mp4"
        assert received[-1].task_state == 1
        assert reporter.diagnostics()["connected"]
    finally:
        reporter.close()
        subscriber.undeclare()
        session.close()


def json_string(value):
    import json

    return json.dumps(value)
