import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import openviking.server.bootstrap as bootstrap


class FakeProcess:
    pid = 4321

    def __init__(self, *, returncode=None):
        self.returncode = returncode
        self.terminated = False
        self.killed = False

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return 0 if self.returncode is None else self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = 0

    def kill(self):
        self.killed = True
        self.returncode = -9


class FakeThread:
    def __init__(self, *, target, args, name, daemon):
        self.target = target
        self.args = args
        self.name = name
        self.daemon = daemon
        self.started = False

    def start(self):
        self.started = True

    def join(self, timeout=None):
        pass


def test_start_expiry_publisher_uses_internal_module_and_resolved_config(monkeypatch):
    captured = {}
    process = FakeProcess()

    def popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return process

    monkeypatch.setattr(bootstrap.subprocess, "Popen", popen)
    monkeypatch.setattr(bootstrap, "_wait_for_expiry_publisher_ready", lambda *_: None)
    monkeypatch.setattr(bootstrap.threading, "Thread", FakeThread)

    managed = bootstrap._start_expiry_publisher(Path("/tmp/ov.conf"))

    assert captured["command"][1:3] == ["-m", "openviking_cli.expiry_bootstrap"]
    assert captured["command"][captured["command"].index("--config") + 1] == "/tmp/ov.conf"
    assert "--startup-status" in captured["command"]
    assert captured["kwargs"]["start_new_session"] == (bootstrap.os.name != "nt")
    assert managed.process is process
    assert managed.monitor_thread is not None
    assert managed.monitor_thread.started is True


def test_monitor_stops_parent_when_publisher_exits_unexpectedly(monkeypatch):
    process = FakeProcess(returncode=7)
    managed = bootstrap.ExpiryPublisherProcess(process)
    signals = []
    monkeypatch.setattr(bootstrap.os, "kill", lambda pid, signum: signals.append((pid, signum)))

    bootstrap._monitor_expiry_publisher(managed)

    assert signals == [(bootstrap.os.getpid(), bootstrap.signal.SIGTERM)]


def test_monitor_ignores_expected_publisher_shutdown(monkeypatch):
    process = FakeProcess(returncode=0)
    managed = bootstrap.ExpiryPublisherProcess(process)
    managed.stopping.set()
    signals = []
    monkeypatch.setattr(bootstrap.os, "kill", lambda pid, signum: signals.append((pid, signum)))

    bootstrap._monitor_expiry_publisher(managed)

    assert signals == []


def test_readiness_surfaces_child_error(tmp_path):
    status_path = tmp_path / "status.json"
    status_path.write_text('{"status":"failed","error":"redis unavailable"}', encoding="utf-8")
    process = SimpleNamespace(poll=lambda: None)

    try:
        bootstrap._wait_for_expiry_publisher_ready(
            process,
            status_path,
            timeout_seconds=0.1,
        )
    except RuntimeError as exc:
        assert "redis unavailable" in str(exc)
    else:
        raise AssertionError("readiness failure was not propagated")


@pytest.mark.skipif(os.name == "nt", reason="SIGTERM lifecycle is POSIX-specific")
def test_sigterm_runs_outer_finally_and_stops_detached_process(tmp_path):
    ready_path = tmp_path / "ready"
    cleanup_path = tmp_path / "cleaned"
    child_pid_path = tmp_path / "child.pid"
    script = textwrap.dedent(
        """
        import subprocess
        import sys
        from contextlib import asynccontextmanager
        from pathlib import Path

        import uvicorn
        from fastapi import FastAPI

        from openviking.server.bootstrap import _sigterm_runs_finally

        ready_path = Path(sys.argv[1])
        cleanup_path = Path(sys.argv[2])
        child_pid_path = Path(sys.argv[3])
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(300)"],
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        child_pid_path.write_text(str(child.pid), encoding="utf-8")

        @asynccontextmanager
        async def lifespan(_app):
            ready_path.write_text("ready", encoding="utf-8")
            yield

        app = FastAPI(lifespan=lifespan)
        try:
            with _sigterm_runs_finally():
                uvicorn.run(app, host="127.0.0.1", port=0, log_level="error")
        finally:
            cleanup_path.write_text("cleaned", encoding="utf-8")
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
        """
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            str(ready_path),
            str(cleanup_path),
            str(child_pid_path),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    child_pid = None
    output = ""
    try:
        deadline = time.monotonic() + 60
        while not ready_path.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert ready_path.exists(), "Uvicorn child did not enter application lifespan"
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))

        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=20)

        assert process.returncode == 128 + signal.SIGTERM, output
        assert cleanup_path.exists(), output
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
