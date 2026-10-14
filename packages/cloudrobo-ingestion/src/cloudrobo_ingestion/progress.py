"""Stable per-file identity, bounded snapshots and independent Zenoh publishing."""

from __future__ import annotations

import collections
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import PurePosixPath

from .store import now_ms


def message_class():
    from cloudrobo_r2c.common.models.generated.data_upload_progress_pb2 import DataUploadProgress

    return DataUploadProgress


def data_type(name):
    ext = PurePosixPath(name).suffix.lower()
    if ext in {".mp4", ".avi", ".mov", ".mkv", ".webm"}:
        return 0
    if ext in {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}:
        return 1
    if ext in {".pcd", ".ply", ".las", ".laz"}:
        return 2
    if ext in {".txt", ".json", ".jsonl", ".csv", ".yaml", ".yml", ".md", ".log"}:
        return 3
    return 4


@dataclass
class Node:
    path: str
    size: int
    file_id: str
    start_time: int = 0
    state: int = 0
    transferred: int = 0
    samples: collections.deque = field(default_factory=collections.deque)

    def update(self, state, transferred=0, at=None):
        at = time.monotonic() if at is None else at
        # OBS can deliver a queued callback after the file completion event.
        if self.state == 2 and state == 1:
            return
        if self.state == state == 1:
            transferred = max(self.transferred, transferred)
        if state == 1 and self.state != 1:
            self.start_time = now_ms()
            self.samples.clear()
            self.samples.append((at, 0))
        self.state = state
        self.transferred = min(self.size, max(0, transferred))
        self.samples.append((at, self.transferred))
        while len(self.samples) > 1 and self.samples[0][0] < at - 5:
            self.samples.popleft()

    def values(self, batch, at):
        rate = 0.0
        if self.state == 1 and self.samples and at - self.samples[-1][0] <= 5:
            oldest = self.samples[0]
            elapsed = at - oldest[0]
            if elapsed > 0:
                rate = max(0, self.transferred - oldest[1]) / elapsed
        estimate = (self.size - self.transferred) * 1000 / rate if rate else 0
        eta_ms = int(estimate) if 0 <= estimate <= 2**63 - 1 else 0
        return dict(
            file_id=self.file_id,
            task_name=PurePosixPath(self.path).name,
            data_type=data_type(self.path),
            size=self.size,
            enqueue_time=batch["timestamp"],
            start_time=self.start_time,
            result_state=self.state,
            progress=100.0
            if self.state == 2
            else (min(99.9, 100 * self.transferred / self.size) if self.size else 0.0),
            avg_bps=rate,
            eta_ms=eta_ms,
            task_type=batch["task_type"],
            collect_method=batch["collect_method"],
        )


class BatchProgress:
    def __init__(self, batch, entries, source, clock=None):
        self.batch, self.source = dict(batch), source
        self.clock = clock or time.monotonic
        self.nodes = {
            e.path: Node(e.path, e.size, str(uuid.uuid5(uuid.UUID(batch["id"]), e.path)))
            for e in entries
        }
        self.finished = False
        self.lock = threading.Lock()

    def update(self, path, state, transferred=0):
        with self.lock:
            self.nodes[path].update(state, transferred, at=self.clock())

    def serialize(self):
        with self.lock:
            at = self.clock()
            values = [n.values(self.batch, at) for n in self.nodes.values()]
            finished = self.finished
        m = message_class()(
            source=self.source,
            timestamp=self.batch["timestamp"],
            asset_path=self.batch["asset_path"],
            task_state=int(finished),
        )
        for value in values:
            m.nodes.add(**value)
        return m.SerializeToString()

    def summary(self):
        with self.lock:
            counts = collections.Counter(n.state for n in self.nodes.values())
            return dict(
                files=len(self.nodes),
                bytes=sum(n.size for n in self.nodes.values()),
                uploaded_bytes=sum(n.transferred for n in self.nodes.values()),
                done_files=counts[2],
                failed_files=counts[3],
                current_files=[n.path for n in self.nodes.values() if n.state == 1],
            )


class Reporter:
    """One slot per batch. Never run transport calls in the upload/control loop."""

    def __init__(self, config_path, topic, factory=None, interval=1.0):
        self.config_path, self.topic, self.interval = config_path, topic, interval
        self.factory = factory or self._transport
        self.lock = threading.Lock()
        self.slots = {}
        self.acks = queue.Queue()
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.status = {"connected": False, "last_published_ms": 0, "error": ""}
        self.thread = threading.Thread(target=self._run, name="ingestion-reporter", daemon=True)

    @staticmethod
    def _transport(path):
        from cloudrobo_r2c.common.config import ClientConfig
        from cloudrobo_r2c.transport import ZenohTransport

        c = ClientConfig.from_yaml(path, None, None, None)
        c.validate()
        t = ZenohTransport()
        try:
            t.connect(c)
        except Exception:
            t.close()
            raise
        return t

    def set(self, batch_id, snapshot, terminal=False):
        with self.lock:
            self.slots[batch_id] = (snapshot, terminal)
        self.wake.set()

    def remove(self, batch_id):
        with self.lock:
            self.slots.pop(batch_id, None)

    def diagnostics(self):
        with self.lock:
            return dict(self.status, enabled=True, pending_batches=len(self.slots))

    def _run(self):
        transport = None
        next_tick, retry_at = 0.0, 0.0
        try:
            while not self.stop_event.is_set():
                now = time.monotonic()
                triggered = self.wake.is_set()
                if now < retry_at or (not triggered and now < next_tick):
                    self.stop_event.wait(min(0.1, max(retry_at, next_tick) - now))
                    continue
                self.wake.clear()
                next_tick = now + self.interval
                with self.lock:
                    slots = dict(self.slots)
                if not slots:
                    self.stop_event.wait(0.1)
                    continue
                try:
                    if transport is None:
                        transport = self.factory(self.config_path)
                    for batch_id, slot in slots.items():
                        if self.stop_event.is_set():
                            break
                        connection_info = getattr(transport, "connection_info", lambda: None)()
                        if connection_info and connection_info.get("connected") is False:
                            raise ConnectionError("Zenoh link disconnected")
                        snapshot, terminal = slot
                        payload = snapshot if isinstance(snapshot, bytes) else snapshot.serialize()
                        transport.publish(self.topic, payload)
                        with self.lock:
                            self.status.update(connected=True, last_published_ms=now_ms(), error="")
                            if terminal and self.slots.get(batch_id) is slot:
                                self.slots.pop(batch_id)
                        self.acks.put((batch_id, payload))
                    retry_at = 0
                except Exception as exc:
                    # Safe diagnostics; endpoints/credentials are not copied into local status.
                    with self.lock:
                        self.status.update(connected=False, error=type(exc).__name__)
                    if transport:
                        try:
                            transport.close()
                        except Exception:
                            pass
                    transport = None
                    retry_at = time.monotonic() + 2
        finally:
            if transport:
                try:
                    transport.close()
                except Exception:
                    pass

    def start(self):
        self.thread.start()

    def close(self):
        self.stop_event.set()
        self.thread.join(timeout=2)


class DisabledReporter:
    """Same supervisor interface, with no serialization, transport or background thread."""

    def __init__(self):
        self.acks = queue.Queue()

    def set(self, *args, **kwargs):
        pass

    def remove(self, *args, **kwargs):
        pass

    def start(self):
        pass

    def close(self):
        pass

    def diagnostics(self):
        return dict(
            enabled=False,
            state="disabled",
            connected=False,
            pending_batches=0,
            last_published_ms=0,
            error="",
        )
