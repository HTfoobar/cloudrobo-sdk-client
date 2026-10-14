"""Real SDK + spawned worker against local OBS multipart protocol simulation."""

import threading
import time
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

import pytest
from cloudrobo_ingestion.daemon import Supervisor
from cloudrobo_ingestion.progress import DisabledReporter, Reporter, message_class


@pytest.mark.parametrize("run_mode", ["upload_and_report", "upload_only"])
def test_real_obs_sdk_in_spawned_worker(config, batch_dir, monkeypatch, run_mode):
    uploaded = []

    class OBSHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, body):
            body = body.encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", '"local-etag"')
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if "uploads" in self.path:
                self.reply(
                    "<InitiateMultipartUploadResult><Bucket>bucket</Bucket>"
                    "<Key>key</Key><UploadId>local-upload-id</UploadId></InitiateMultipartUploadResult>"
                )
            else:
                self.reply(
                    "<CompleteMultipartUploadResult><Location>local</Location>"
                    "<Bucket>bucket</Bucket><Key>key</Key><ETag>local-etag</ETag></CompleteMultipartUploadResult>"
                )

        def do_PUT(self):
            data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            uploaded.append((unquote(self.path), data))
            self.reply("")

        def do_DELETE(self):
            self.reply("")

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), OBSHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("HUAWEI_CLOUD_AK", "test-ak")
    monkeypatch.setenv("HUAWEI_CLOUD_SK", "test-sk")
    monkeypatch.setenv("CLOUDROBO_OBS_ENDPOINT", f"http://127.0.0.1:{server.server_port}")
    config = replace(config, run_mode=run_mode)
    if run_mode == "upload_only":
        from cloudrobo_ingestion import progress

        def forbidden():
            raise AssertionError("upload_only serialized protobuf")

        monkeypatch.setattr(progress, "message_class", forbidden)
    reporter = Reporter("unused", config.topic) if config.reports else DisabledReporter()
    s = Supervisor(config, reporter, config.work_dir / "status.json")
    try:
        s.initialize()
        s.scan()
        s.launch_next()
        deadline = time.monotonic() + 15
        while s.process is not None and time.monotonic() < deadline:
            s.drain()
            time.sleep(0.02)
        assert s.process is None
        batch = s.store.rows()[0]
        assert batch["state"] == "done", batch["error"]
        assert uploaded[0][1] == b"customer-data"
        assert "/prefix/批次001/data/file-000.parquet" in uploaded[0][0]
        if config.reports:
            assert message_class().FromString(batch["pending"]).nodes[0].progress == 100
        else:
            assert batch["pending"] is None
            assert s.snapshot()["reporter"]["enabled"] is False
        assert not (config.work_dir / "staging" / batch["id"]).exists()
        assert not (config.work_dir / "checkpoints" / f"{batch['id']}.json").exists()
    finally:
        if s.process:
            s.process.terminate()
            s.process.join(timeout=2)
            s.pipe.close()
        s.store.close()
        server.shutdown()
        server.server_close()
