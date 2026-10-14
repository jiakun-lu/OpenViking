import asyncio
import json
import logging
import subprocess
import sys

import pytest

from openviking.scheduling.expiry import runtime as runtime_module
from openviking_cli import expiry_bootstrap


def write_config(tmp_path, *, enabled=True, idle_backend="redis", separate=True):
    path = tmp_path / "ov.conf"
    path.write_text(
        json.dumps(
            {
                "memory": {
                    "session_auto_commit": {
                        "enabled": enabled,
                        "idle_backend": idle_backend,
                    }
                },
                "expiry": {
                    "service_name": "documents",
                    "publisher_separate_process": separate,
                    "redis": {
                        "mode": "cluster",
                        "endpoints": ["redis://proxy.example.com:6379"],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_internal_publisher_loads_validated_expiry_config(tmp_path):
    config = expiry_bootstrap.load_expiry_config(write_config(tmp_path))

    assert config.service_name == "documents"
    assert config.redis.mode == "cluster"


@pytest.mark.parametrize(
    "options",
    [
        {"enabled": False},
        {"idle_backend": "local"},
        {"separate": False},
    ],
)
def test_internal_publisher_rejects_configuration_that_does_not_select_it(tmp_path, options):
    with pytest.raises(ValueError):
        expiry_bootstrap.load_expiry_config(write_config(tmp_path, **options))


def test_startup_status_is_replaced_atomically(tmp_path):
    status_path = tmp_path / "status.json"

    expiry_bootstrap.write_startup_status(status_path, "failed", "redis unavailable")

    assert json.loads(status_path.read_text(encoding="utf-8"))["error"] == "redis unavailable"
    assert list(tmp_path.glob("*.tmp")) == []


@pytest.mark.asyncio
async def test_publisher_closes_runtime_before_joining_wait_task(monkeypatch, tmp_path):
    instances = []

    class Runtime:
        def __init__(self, config, *, roles):
            self.stopped = asyncio.Event()
            self.close_calls = 0
            instances.append(self)

        async def start_storage(self):
            pass

        async def start_roles(self):
            pass

        async def wait(self):
            await self.stopped.wait()

        async def close(self):
            self.close_calls += 1
            self.stopped.set()

    monkeypatch.setattr(runtime_module, "ExpiryRuntime", Runtime)
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(
        expiry_bootstrap.run_publisher(object(), stop, tmp_path / "status.json"),
        timeout=1.0,
    )

    assert instances[0].close_calls == 1


@pytest.mark.parametrize("password", ["short-secret", "private-password-" * 40])
@pytest.mark.parametrize("failure", ["endpoint", "password_sources", "unrelated"])
def test_publisher_config_failure_logs_and_status_do_not_expose_password(
    tmp_path, caplog, password, failure
):
    config_path = write_config(tmp_path)
    data = json.loads(config_path.read_text())
    data["expiry"]["redis"]["password"] = password
    if failure == "endpoint":
        data["expiry"]["redis"]["endpoints"] = ["invalid-endpoint"]
    elif failure == "password_sources":
        data["expiry"]["redis"]["password_env"] = "REDIS_PASSWORD"
    else:
        data["default_account"] = []
    config_path.write_text(json.dumps(data))
    status = tmp_path / "status.json"
    with caplog.at_level(logging.ERROR):
        result = expiry_bootstrap.main(
            ["--config", str(config_path), "--startup-status", str(status)]
        )
    assert result == 1
    assert password not in caplog.text
    assert password not in status.read_text()
    assert json.loads(status.read_text())["status"] == "failed"
    assert "configuration failed" in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


def test_publisher_runtime_failure_retains_exception_logging(monkeypatch, tmp_path, caplog):
    config_path = write_config(tmp_path)

    async def fail(*args):
        raise RuntimeError("runtime failure")

    monkeypatch.setattr(expiry_bootstrap, "run_publisher", fail)
    with caplog.at_level(logging.ERROR):
        result = expiry_bootstrap.main(
            ["--config", str(config_path), "--startup-status", str(tmp_path / "status")]
        )
    assert result == 1
    assert "runtime failure" in caplog.text
    assert any(record.exc_info is not None for record in caplog.records)


def test_module_entrypoint_invokes_main():
    result = subprocess.run(
        [sys.executable, "-m", "openviking_cli.expiry_bootstrap", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--startup-status" in result.stdout
