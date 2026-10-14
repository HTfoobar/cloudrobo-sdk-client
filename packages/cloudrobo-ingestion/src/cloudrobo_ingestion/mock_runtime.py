"""In-memory simulated uploads. Never opens a source tree or an OBS client."""

import os
import queue
import threading
import time
import uuid

from .filesystem import FileEntry
from .lifecycle import write_json
from .progress import BatchProgress
from .store import now_ms
from .workspace import bind_mode


class MockRunner:
    def __init__(self, config, reporter, status_path, clock=None):
        if config.run_mode != "mock_report":
            raise ValueError("MockRunner 仅支持 mock_report")
        self.config, self.reporter, self.status_path = config, reporter, status_path
        self.clock = clock or time.monotonic
        self.stop_event = threading.Event()
        self.progress = None
        self.phase = "ready"
        self.file_index = 0
        self.started_at = 0.0
        self.next_batch_at = 0.0
        self.completed = 0
        self.final_payload = None
        bind_mode(config)

    def _new_batch(self):
        batch_id = str(uuid.uuid4())
        batch = dict(
            id=batch_id,
            timestamp=now_ms(),
            task_type=self.config.task_type,
            collect_method=self.config.collect_method,
            asset_path=self.config.obs_path + f"mock-{batch_id}/",
        )
        entries = [
            FileEntry(f"mock-{i + 1:05d}.mp4", self.config.mock["file_size_bytes"], 0, 0)
            for i in range(self.config.mock["file_count"])
        ]
        source = f"{self.config.zenoh['project_id']}/{self.config.zenoh['device_id']}"
        self.progress = BatchProgress(batch, entries, source, clock=self.clock)
        self.paths = list(self.progress.nodes)
        self.file_index = 0
        self.phase = "queued"
        self.final_payload = None
        self.reporter.set(batch_id, self.progress)

    def tick(self):
        """Advance simulation using elapsed monotonic time; usable with a fake clock."""
        now = self.clock()
        while True:
            try:
                batch_id, payload = self.reporter.acks.get_nowait()
            except queue.Empty:
                break
            if (
                self.phase == "awaiting_publish"
                and batch_id == self.progress.batch["id"]
                and payload == self.final_payload
            ):
                self.completed += 1
                self.reporter.remove(batch_id)
                self.phase = "waiting"
                self.next_batch_at = now + self.config.mock["batch_interval_seconds"]
        if self.stop_event.is_set():
            return
        if self.phase in ("ready", "waiting"):
            if now >= self.next_batch_at:
                self._new_batch()
            return
        if self.phase == "awaiting_publish":
            return
        path = self.paths[self.file_index]
        if self.phase == "queued":
            self.started_at = now
            self.progress.update(path, 1, 0)
            self.phase = "uploading"
            self.reporter.set(self.progress.batch["id"], self.progress)
            return
        size = self.config.mock["file_size_bytes"]
        amount = int(min(size, max(0, now - self.started_at) * self.config.mock["upload_bps"]))
        self.progress.update(path, 1, amount)
        if amount < size:
            return
        self.progress.update(path, 2, size)
        self.file_index += 1
        if self.file_index < len(self.paths):
            self.phase = "queued"
            self.reporter.set(self.progress.batch["id"], self.progress)
            return
        with self.progress.lock:
            self.progress.finished = True
        self.final_payload = self.progress.serialize()
        self.phase = "awaiting_publish"
        self.reporter.set(self.progress.batch["id"], self.final_payload, terminal=True)

    def snapshot(self, state="running"):
        return dict(
            state=state,
            run_mode=self.config.run_mode,
            mock=True,
            pid=os.getpid(),
            updated_ms=now_ms(),
            work_dir=str(self.config.work_dir),
            source_dir=None,
            obs_path=self.config.obs_path,
            mock_phase=self.phase,
            counts=dict(done=self.completed),
            active_batch=self.progress.batch["id"] if self.progress else None,
            progress=self.progress.summary() if self.progress else {},
            reporter=self.reporter.diagnostics(),
            error="",
            batch_errors=[],
        )

    def run(self):
        next_status = 0
        try:
            while not self.stop_event.is_set():
                self.tick()
                if self.clock() >= next_status:
                    write_json(self.status_path, self.snapshot())
                    next_status = self.clock() + 1
                self.stop_event.wait(0.05)
        finally:
            self.reporter.close()
            write_json(self.status_path, self.snapshot("stopped"))
