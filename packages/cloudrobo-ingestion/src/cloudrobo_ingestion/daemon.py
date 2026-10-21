"""Local supervisor. Database and durable state have exactly one writer."""

from __future__ import annotations

import collections
import contextlib
import json
import logging
import multiprocessing
import os
import queue
import signal
import sys
import tempfile
import threading
import time
from logging.handlers import RotatingFileHandler
from multiprocessing.connection import Listener
from pathlib import Path

import yaml

from .config import IngestionConfig
from .filesystem import RejectedBatch, discover, manifest, remove_staging
from .lifecycle import InstanceLock, private_dir, runtime_dir, write_json
from .progress import BatchProgress, DisabledReporter, Reporter, message_class
from .store import BatchStore, now_ms
from .worker import run_worker

logger = logging.getLogger(__name__)


class Supervisor:
    def __init__(self, config, reporter, status_path, worker_target=run_worker):
        self.config, self.reporter, self.status_path = config, reporter, status_path
        self.worker_target = worker_target
        for name in ("staging", "checkpoints"):
            private_dir(config.work_dir / name)
        self.store = BatchStore(config)
        self.store.recover()
        self.progress = {}
        self.stop_event = threading.Event()
        self.process = self.pipe = None
        self.active = self.result = None
        self.cleanup_only = False
        self.last_error = ""
        self.context = multiprocessing.get_context("spawn")

    def cleanup(self, batch):
        try:
            remove_staging(self.config.work_dir / "staging" / batch["id"])
            pending = (self.config.work_dir / "checkpoints" / f"{batch['id']}.json").exists()
            self.store.update(batch["id"], cleanup_pending=int(pending))
            return not pending
        except Exception as exc:
            self.last_error = f"工作副本清理失败: {batch['path']} ({type(exc).__name__})"
            self.store.update(batch["id"], cleanup_pending=1, error=self.last_error)
            return False

    def initialize(self):
        for batch in self.store.rows():
            self.cleanup(batch)
            if self.config.reports and batch["pending"] is not None:
                self.reporter.set(batch["id"], batch["pending"], terminal=True)

    def scan(self):
        for source in discover(self.config.source_dir):
            if self.stop_event.is_set():
                break
            relative = source.relative_to(self.config.source_dir).as_posix()
            batch = self.store.add(relative, self.config)
            if batch["state"] in ("done", "rejected") or batch["id"] in self.progress:
                continue
            try:
                entries = manifest(source, self.config.max_files_per_batch)
                progress = BatchProgress(batch, entries, self.source)
                self.progress[batch["id"]] = progress
                self.store.update(
                    batch["id"], files=len(entries), bytes=sum(e.size for e in entries)
                )
                self.reporter.set(batch["id"], progress)
            except RejectedBatch as exc:
                self.store.update(batch["id"], state="rejected", error=str(exc), pending=None)
                self.reporter.remove(batch["id"])
            except OSError as exc:
                self.store.update(
                    batch["id"],
                    state="failed",
                    retry_at=time.time() + 5,
                    error=f"源目录读取失败: {type(exc).__name__}",
                )

    @property
    def source(self):
        if not self.config.reports:
            return ""
        return f"{self.config.zenoh['project_id']}/{self.config.zenoh['device_id']}"

    def launch_next(self):
        for batch in self.store.rows():
            if self.stop_event.is_set():
                return
            if batch["retry_at"] > time.time() or batch["state"] == "rejected":
                continue
            if batch["cleanup_pending"]:
                # Retry local cleanup; if an OBS checkpoint remains, ask the child
                # to abort it without blocking the supervisor/control loop.
                self.cleanup(batch)
                stage = self.config.work_dir / "staging" / batch["id"]
                if stage.exists() or stage.is_symlink():
                    self.store.update(batch["id"], retry_at=time.time() + 5)
                    continue
                self._launch(batch, [], cleanup_only=True)
                return
            if batch["state"] == "done":
                continue
            try:
                entries = manifest(
                    self.config.source_dir / batch["path"], self.config.max_files_per_batch
                )
            except RejectedBatch as exc:
                self.store.update(batch["id"], state="rejected", error=str(exc), pending=None)
                self.progress.pop(batch["id"], None)
                self.reporter.remove(batch["id"])
                continue
            except OSError as exc:
                self.store.update(
                    batch["id"],
                    state="failed",
                    retry_at=time.time() + 5,
                    error=f"源目录读取失败: {type(exc).__name__}",
                )
                continue
            p = BatchProgress(batch, entries, self.source)
            self.progress[batch["id"]] = p
            self.reporter.set(batch["id"], p)
            self.store.update(
                batch["id"],
                state="copying",
                attempts=batch["attempts"] + 1,
                files=len(entries),
                bytes=sum(e.size for e in entries),
                pending=None,
                cleanup_pending=1,
                error="",
            )
            self._launch(batch, entries)
            return

    def _launch(self, batch, entries, cleanup_only=False):
        parent, child = self.context.Pipe()
        self.active, self.result, self.cleanup_only = batch["id"], None, cleanup_only
        self.pipe = parent
        self.process = self.context.Process(
            target=self.worker_target,
            args=(self.config, batch, entries, child, cleanup_only),
            daemon=True,
        )
        try:
            self.process.start()
        except Exception:
            parent.close()
            self.process = self.pipe = None
            self.store.update(
                batch["id"], state="failed", retry_at=time.time() + 5, error="上传子进程启动失败"
            )
            raise
        finally:
            child.close()

    def _drain_worker_events(self):
        if self.pipe:
            try:
                while self.pipe.poll():
                    event = self.pipe.recv()
                    kind = event["event"]
                    if kind == "file":
                        self.progress[self.active].update(
                            event["path"], event["state"], event["transferred"]
                        )
                    elif kind == "phase":
                        self.store.update(self.active, state=event["state"])
                    elif kind == "log":
                        logging.getLogger("cloudrobo_ingestion.worker").log(
                            {
                                "INFO": logging.INFO,
                                "WARNING": logging.WARNING,
                                "ERROR": logging.ERROR,
                            }.get(event["level"], logging.INFO),
                            "%s",
                            event["message"],
                        )
                    elif kind in ("result", "cleaned"):
                        self.result = event
            except (EOFError, OSError):
                pass

    def drain(self):
        self._drain_worker_events()
        if self.process and not self.process.is_alive():
            self.process.join()
            # The child may have sent its final log/result after the first poll.
            self._drain_worker_events()
            self._finish()
        while True:
            try:
                batch_id, payload = self.reporter.acks.get_nowait()
            except queue.Empty:
                break
            self.store.acknowledge(batch_id, payload)

    def _finish(self):
        batch = self.store.get(self.active)
        result = self.result or dict(state="interrupted", error="上传子进程异常退出")
        if self.cleanup_only:
            if result.get("event") == "cleaned":
                self.store.update(self.active, cleanup_pending=0, retry_at=0)
            else:
                self.store.update(
                    self.active,
                    cleanup_pending=1,
                    retry_at=time.time() + 5,
                    error=result.get("error", "清理失败"),
                )
        else:
            state = result["state"]
            p = self.progress.get(self.active)
            if p:
                with p.lock:
                    p.finished = state == "done"
                    for node in p.nodes.values():
                        if node.state == 1:
                            node.state = 0 if state == "interrupted" else 3
                        if state == "rejected":
                            node.state = 3
                payload = p.serialize() if self.config.reports else None
            else:
                payload = None
            delay = (
                min(300, 5 * 2 ** min(max(batch["attempts"] - 1, 0), 6)) if state == "failed" else 0
            )
            self.store.update(
                self.active,
                state=state,
                error=result.get("error", ""),
                retry_at=time.time() + delay,
                pending=payload,
            )
            if payload is not None:
                self.reporter.set(
                    self.active, payload if state == "done" else p, terminal=state == "done"
                )
            if state == "done":
                self.progress.pop(self.active, None)
            self.cleanup(self.store.get(self.active))
            if result.get("error"):
                self.last_error = result["error"]
                logger.warning("Batch %s: %s", batch["path"], self.last_error)
        self.pipe.close()
        self.process.close()
        self.process = self.pipe = self.active = self.result = None

    def snapshot(self, state="running"):
        rows = self.store.rows()
        counts = dict(collections.Counter(r["state"] for r in rows))
        active = self.store.get(self.active) if self.active else None
        return dict(
            state=state,
            run_mode=self.config.run_mode,
            pid=os.getpid(),
            updated_ms=now_ms(),
            source_dir=str(self.config.source_dir),
            work_dir=str(self.config.work_dir),
            obs_path=self.config.obs_path,
            counts=counts,
            active_batch=active["path"] if active else None,
            progress=self.progress[self.active].summary() if self.active in self.progress else {},
            error=self.last_error,
            reporter=self.reporter.diagnostics(),
            batch_errors=[
                {"path": r["path"], "state": r["state"], "error": r["error"]}
                for r in rows
                if r["error"]
            ][-50:],
        )

    def run(self):
        self.initialize()
        next_scan = next_status = 0
        try:
            while not self.stop_event.is_set():
                self.drain()
                now = time.monotonic()
                if now >= next_scan:
                    try:
                        self.scan()
                    except (OSError, RejectedBatch) as exc:
                        self.last_error = str(exc)
                    next_scan = now + 2
                if self.process is None:
                    self.launch_next()
                if now >= next_status:
                    write_json(self.status_path, self.snapshot())
                    next_status = now + 1
                self.stop_event.wait(0.05)
        finally:
            if self.process:
                with contextlib.suppress(OSError):
                    self.pipe.send("stop")
                deadline = time.monotonic() + 5
                while self.process and self.process.is_alive() and time.monotonic() < deadline:
                    self.drain()
                    time.sleep(0.05)
                if self.process:
                    if self.process.is_alive():
                        self.process.terminate()
                        self.process.join(timeout=2)
                        if self.process.is_alive():
                            self.process.kill()
                            self.process.join()
                    self.drain()
            self.reporter.close()
            self.drain()
            write_json(self.status_path, self.snapshot("stopped"))
            self.store.close()


def main(config_path, handshake, effective_path=None):
    root = runtime_dir()
    private_dir(root)
    z_path = None
    supervisor = listener = None
    try:
        with InstanceLock(root / "instance.lock"):
            config = (
                IngestionConfig.from_mapping(
                    json.loads(Path(effective_path).read_text(encoding="utf-8")),
                    Path(config_path).resolve().parent,
                )
                if effective_path
                else IngestionConfig.load(config_path)
            )
            private_dir(config.work_dir)
            from .workspace import check_mode

            check_mode(config)
            handler = RotatingFileHandler(
                config.work_dir / "ingestion.log",
                maxBytes=5 * 1024 * 1024,
                backupCount=3,
                encoding="utf-8",
            )
            logging.basicConfig(
                level=logging.INFO,
                handlers=[handler],
                format="%(asctime)s %(levelname)s %(name)s %(message)s",
            )
            logger.info("Starting ingestion run_mode=%s", config.run_mode)
            if config.uploads:
                from cloudrobo_core.sdk import Config

                credentials = Config()
                if not credentials.ak or not credentials.sk:
                    raise ValueError("OBS 凭证缺失，请配置 HUAWEI_CLOUD_AK / HUAWEI_CLOUD_SK")
            # Fail early on missing generated protobuf/runtime libraries, even
            # if the first batch has not arrived yet. Router availability is optional.
            reporter = DisabledReporter()
            if config.reports:
                message_class()
                from cloudrobo_r2c.common.config import ClientConfig
                from cloudrobo_r2c.transport import ZenohTransport

                del ZenohTransport
                fd, name = tempfile.mkstemp(prefix="zenoh-", suffix=".yaml", dir=config.work_dir)
                z_path = Path(name)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    z = dict(config.zenoh)
                    z.setdefault("publisher_priority", "BACKGROUND")
                    z.setdefault("publisher_congestion_control", "DROP")
                    z.setdefault(
                        "connect",
                        dict(endpoints=z["endpoints"], timeout_ms=2000, exit_on_failure=True),
                    )
                    yaml.safe_dump(z, f, allow_unicode=True)
                ClientConfig.from_yaml(z_path, None, None, None).validate()
                reporter = Reporter(z_path, config.topic)
            if config.run_mode == "mock_report":
                from .mock_runtime import MockRunner

                supervisor = MockRunner(config, reporter, root / "status.json")
            else:
                supervisor = Supervisor(config, reporter, root / "status.json")
            token = os.urandom(32)
            listener = Listener(("127.0.0.1", 0), authkey=token)
            write_json(root / "control.json", dict(port=listener.address[1], token=token.hex()))

            def control():
                while not supervisor.stop_event.is_set():
                    try:
                        c = listener.accept()
                        try:
                            if c.poll(2) and c.recv() == "stop":
                                supervisor.stop_event.set()
                                c.send("stopping")
                        finally:
                            c.close()
                    except (OSError, EOFError, multiprocessing.AuthenticationError):
                        if supervisor.stop_event.is_set():
                            break

            threading.Thread(target=control, name="ingestion-control", daemon=True).start()
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: supervisor.stop_event.set())
            reporter.start()
            write_json(root / "status.json", supervisor.snapshot())
            write_json(
                handshake,
                dict(pid=os.getpid(), work_dir=str(config.work_dir), run_mode=config.run_mode),
            )
            supervisor.run()
    except Exception as exc:
        logger.exception("Ingestion daemon failed")
        if not Path(handshake).exists():
            write_json(handshake, dict(error=str(exc)))
        raise
    finally:
        if listener:
            listener.close()
        if z_path:
            z_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None)
