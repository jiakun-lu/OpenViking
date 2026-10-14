# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Service-level idle checks and best-effort acceptance metadata recording."""

import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.core.namespace import canonical_session_uri
from openviking.scheduling.expiry.config import ExpiryConfig
from openviking.service.session_service import SessionService
from openviking.session import Session
from openviking.session.session import SessionMeta
from openviking.utils.time_utils import parse_iso_datetime
from openviking_cli.exceptions import NotFoundError
from openviking_cli.utils.config.memory_config import SessionAutoCommitConfig


class MetadataFS:
    """Minimal storage with observable locking and controllable concurrent writes."""

    def __init__(self, ctx):
        self.session_uri = canonical_session_uri(ctx, "s")
        self.meta_uri = f"{self.session_uri}/.meta.json"
        self.meta = {
            "auto_commit_policy": {"idle_timeout_seconds": 60, "keep_recent_count": 12},
            "last_message_at": "2026-01-01T00:00:00Z",
            "last_idle_commit_accepted_at": "",
            "message_count": 1,
            "commit_count": 3,
            "updated_at": "2026-01-01T00:00:00Z",
        }
        self.exists = True
        self.locked = False
        self.events = []
        self.writes = []
        self.on_acquire = None
        self.read_error = None
        self.write_error = None
        self.raw_meta = None
        self._async_agfs = SimpleNamespace(
            pathlock_acquire_exact=self.acquire,
            pathlock_release=self.release,
        )

    def _uri_to_path(self, uri, *, ctx):
        assert uri == self.session_uri
        return "/local/session/s"

    async def acquire(self, path, **kwargs):
        assert path == "/local/session/s"
        assert not self.locked
        self.locked = True
        self.events.append("acquire")
        if self.on_acquire is not None:
            self.on_acquire()
        return "lease"

    async def release(self, lease):
        assert lease == "lease"
        assert self.locked
        self.events.append("release")
        self.locked = False

    async def stat(self, uri, *, ctx, **kwargs):
        assert uri == self.session_uri
        self.events.append("stat")
        if not self.exists:
            raise NotFoundError("s", "session")
        return {"isDir": True}

    async def read_file(self, uri, *, ctx, **kwargs):
        assert uri == self.meta_uri
        assert self.locked, "Acceptance metadata must be re-read under the session lock"
        self.events.append("read")
        if not self.exists:
            raise NotFoundError("s", "session")
        if self.read_error is not None:
            raise self.read_error
        return self.raw_meta if self.raw_meta is not None else json.dumps(self.meta)

    async def write_file(self, uri, content, *, ctx, **kwargs):
        assert uri == self.meta_uri
        assert self.locked
        assert kwargs.get("lease_ref") is None, (
            "Child .meta.json must take its own auto-pathlock under the held "
            "session-directory lock, not reuse the directory lease"
        )
        assert self.exists, "Recording acceptance must not recreate a deleted session"
        self.events.append("write")
        if self.write_error is not None:
            raise self.write_error
        self.meta = json.loads(content)
        self.writes.append(copy.deepcopy(self.meta))


@pytest.fixture
def idle_session():
    return SimpleNamespace(
        session_id="s",
        messages=[object()],
        meta=SessionMeta(
            session_id="s",
            auto_commit_policy={"idle_timeout_seconds": 60, "keep_recent_count": 12},
            last_message_at="2026-01-01T00:00:00Z",
            message_count=1,
            keep_recent_count=12,
        ),
        commit_async=AsyncMock(spec=Session.commit_async.__get__(object(), Session)),
    )


@pytest.fixture
def timeout_service(idle_session, request_context, monkeypatch):
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 1, 1, 0, 2, tzinfo=timezone.utc)
            return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr("openviking.service.session_service.datetime", FrozenDatetime)
    fs = MetadataFS(request_context)
    service = SessionService(viking_fs=fs)
    service.get = AsyncMock(spec=service.get, return_value=idle_session)
    return service, fs


@pytest.fixture
def now():
    return datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc)


@pytest.mark.parametrize("policy", [None, {"idle_timeout_seconds": 0}])
def test_timeout_check_disabled_policy_is_pure(idle_session, now, policy):
    idle_session.meta.auto_commit_policy = policy
    idle_session.meta.last_message_at = ""
    before = copy.deepcopy(idle_session.meta.to_dict())

    result = SessionService._check_timeout_commit(idle_session, now)

    assert result == {"status": "skipped", "reason": "idle_disabled"}
    assert idle_session.meta.to_dict() == before
    idle_session.commit_async.assert_not_called()


def test_timeout_check_empty_messages_precedes_cooldown_and_activity(idle_session, now):
    idle_session.messages = []
    idle_session.meta.message_count = 99
    idle_session.meta.last_idle_commit_accepted_at = now.isoformat()
    idle_session.meta.last_message_at = ""

    assert SessionService._check_timeout_commit(idle_session, now) == {
        "status": "skipped",
        "reason": "no_messages",
    }


def test_timeout_check_uses_messages_not_keep_window_or_pending_tokens(idle_session, now):
    idle_session.meta.message_count = 0
    idle_session.meta.pending_tokens = 0
    idle_session.meta.keep_recent_count = 100

    assert SessionService._check_timeout_commit(idle_session, now) is None


@pytest.mark.parametrize("offset_ms", [-1, 0, 1])
@pytest.mark.parametrize(
    "last_message_at",
    ["2026-01-01T00:01:00Z", "2026-01-01T00:01:00", "2026-01-01T08:01:00+08:00"],
)
def test_timeout_check_activity_boundary(idle_session, now, offset_ms, last_message_at):
    idle_session.meta.last_message_at = last_message_at
    before = copy.deepcopy(idle_session.meta.to_dict())
    result = SessionService._check_timeout_commit(
        idle_session, now + timedelta(milliseconds=offset_ms)
    )

    if offset_ms < 0:
        assert result == {
            "status": "deferred",
            "reason": "idle_activity",
            "timeout_seconds": 60,
            "due_at_ms": int(now.timestamp() * 1000),
        }
    else:
        assert result is None
    assert idle_session.meta.to_dict() == before


@pytest.mark.parametrize("offset_ms", [-1, 0, 1])
def test_timeout_check_cooldown_boundary(idle_session, now, offset_ms):
    idle_session.meta.last_idle_commit_accepted_at = "2026-01-01T00:01:00Z"
    checked_at = now + timedelta(milliseconds=offset_ms)

    result = SessionService._check_timeout_commit(idle_session, checked_at)

    if offset_ms < 0:
        assert result == {
            "status": "deferred",
            "reason": "idle_cooldown",
            "timeout_seconds": 60,
            "due_at_ms": int(checked_at.timestamp() * 1000) + 60_000,
        }
    else:
        assert result is None


def test_timeout_check_cooldown_uses_current_timeout_and_precedes_activity(idle_session, now):
    idle_session.meta.auto_commit_policy = {"idle_timeout_seconds": 120}
    idle_session.meta.last_idle_commit_accepted_at = "2026-01-01T00:00:30Z"
    idle_session.meta.last_message_at = "2026-01-01T00:01:59Z"
    idle_session.meta.last_auto_commit_at = "2000-01-01T00:00:00Z"

    assert SessionService._check_timeout_commit(idle_session, now) == {
        "status": "deferred",
        "reason": "idle_cooldown",
        "timeout_seconds": 120,
        "due_at_ms": int(now.timestamp() * 1000) + 120_000,
    }


def test_timeout_check_legacy_auto_commit_time_does_not_start_idle_cooldown(idle_session, now):
    idle_session.meta.last_auto_commit_at = now.isoformat()

    assert SessionService._check_timeout_commit(idle_session, now) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("working_memory_enabled", [None, False, True])
async def test_timeout_commit_delegates_original_commit_and_records_after_acceptance(
    timeout_service, idle_session, request_context, monkeypatch, working_memory_enabled
):
    service, fs = timeout_service
    idle_session.meta.memory_policy = (
        None
        if working_memory_enabled is None
        else {"working_memory": {"enabled": working_memory_enabled}}
    )
    result = {"status": "accepted", "archived": True, "task_id": "task", "archive_uri": "archive"}
    accepted_at = "2026-01-01T00:03:00.123Z"

    async def commit(**kwargs):
        fs.events.append("commit")
        return result

    clock_calls = 0

    def acceptance_clock():
        nonlocal clock_calls
        if clock_calls == 0:
            assert fs.events == ["commit"], "Acceptance timestamp is sampled after commit returns"
        clock_calls += 1
        return accepted_at

    idle_session.commit_async.side_effect = commit
    monkeypatch.setattr(
        "openviking.service.session_service.get_current_timestamp", acceptance_clock
    )

    actual = await service.commit_on_timeout("s", request_context)

    assert actual is result
    service.get.assert_awaited_once_with("s", request_context, auto_create=False)
    idle_session.commit_async.assert_awaited_once_with(
        keep_recent_count=0,
        persist_keep_recent_count=False,
        record_auto_commit_success=True,
    )
    assert fs.meta["last_idle_commit_accepted_at"] == accepted_at
    assert len(fs.writes) == 1
    assert fs.events.index("commit") < fs.events.index("acquire") < fs.events.index("read")
    assert fs.events[-1] == "release"


@pytest.mark.asyncio
async def test_timeout_commit_does_not_recheck_activity_after_outer_check(
    timeout_service, idle_session, request_context
):
    service, fs = timeout_service
    result = {"status": "accepted", "archived": True}

    async def append_then_original_commit(**kwargs):
        idle_session.messages.append(object())
        idle_session.meta.last_message_at = datetime.now(timezone.utc).isoformat()
        return result

    idle_session.commit_async.side_effect = append_then_original_commit

    assert await service.commit_on_timeout("s", request_context) is result
    assert len(idle_session.messages) == 2
    idle_session.commit_async.assert_awaited_once_with(
        keep_recent_count=0,
        persist_keep_recent_count=False,
        record_auto_commit_success=True,
    )
    assert len(fs.writes) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["idle_disabled", "no_messages"])
async def test_timeout_commit_outer_skip_does_not_commit_or_record(
    timeout_service, idle_session, request_context, reason
):
    service, fs = timeout_service
    if reason == "idle_disabled":
        idle_session.meta.auto_commit_policy = None
    else:
        idle_session.messages = []

    assert await service.commit_on_timeout("s", request_context) == {
        "status": "skipped",
        "reason": reason,
    }
    idle_session.commit_async.assert_not_awaited()
    assert fs.events == []
    assert fs.writes == []


@pytest.mark.asyncio
async def test_timeout_commit_skipped_original_commit_does_not_record(
    timeout_service, idle_session, request_context
):
    service, fs = timeout_service
    result = {"status": "skipped", "reason": "no_messages", "archived": False}
    idle_session.commit_async.return_value = result

    assert await service.commit_on_timeout("s", request_context) is result
    idle_session.commit_async.assert_awaited_once()
    assert fs.events == []
    assert fs.writes == []


@pytest.mark.asyncio
async def test_timeout_commit_not_found_loading_is_skipped(timeout_service, request_context):
    service, fs = timeout_service
    service.get.side_effect = NotFoundError("s", "session")

    assert await service.commit_on_timeout("s", request_context) == {
        "status": "skipped",
        "reason": "not_found",
    }
    service.get.assert_awaited_once_with("s", request_context, auto_create=False)
    assert fs.events == []


@pytest.mark.asyncio
async def test_timeout_commit_other_load_errors_propagate(timeout_service, request_context):
    service, fs = timeout_service
    error = RuntimeError("metadata storage unavailable")
    service.get.side_effect = error

    with pytest.raises(RuntimeError, match="metadata storage unavailable") as caught:
        await service.commit_on_timeout("s", request_context)

    assert caught.value is error
    assert fs.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("not_found", [False, True])
async def test_timeout_commit_commit_errors_propagate_without_recording(
    timeout_service, idle_session, request_context, not_found
):
    service, fs = timeout_service
    error = NotFoundError("archive", "archive") if not_found else RuntimeError("commit failed")
    idle_session.commit_async.side_effect = error

    with pytest.raises(type(error)) as caught:
        await service.commit_on_timeout("s", request_context)

    assert caught.value is error
    idle_session.commit_async.assert_awaited_once()
    assert fs.events == []


@pytest.fixture
def service_logs(caplog):
    from openviking.service.session_service import logger

    logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


@pytest.mark.asyncio
async def test_timeout_commit_metadata_write_failure_still_returns_accepted(
    timeout_service, idle_session, request_context, service_logs
):
    service, fs = timeout_service
    fs.write_error = RuntimeError("acceptance metadata write failed")
    result = {"status": "accepted", "archived": True, "task_id": "task"}
    idle_session.commit_async.return_value = result

    assert await service.commit_on_timeout("s", request_context) is result
    assert "write" in fs.events
    assert fs.events[-1] == "release"
    assert fs.writes == []
    assert "Failed to record idle commit time" in service_logs.text


@pytest.mark.asyncio
async def test_record_idle_time_rereads_under_lock_and_preserves_concurrent_fields(
    timeout_service, request_context
):
    service, fs = timeout_service
    concurrent = {
        **fs.meta,
        "auto_commit_policy": {"idle_timeout_seconds": 180, "keep_recent_count": 77},
        "last_message_at": "2026-01-01T00:02:59Z",
        "message_count": 9,
        "commit_count": 4,
        "pending_tokens": 4321,
        "future_extension": {"opaque": [1, 2, 3]},
    }
    fs.on_acquire = lambda: setattr(fs, "meta", copy.deepcopy(concurrent))
    accepted_at = "2026-01-01T00:03:00.123Z"

    await service._record_idle_commit_time("s", request_context, accepted_at)

    assert fs.meta["last_idle_commit_accepted_at"] == accepted_at
    assert parse_iso_datetime(fs.meta["updated_at"]) >= parse_iso_datetime(concurrent["updated_at"])
    assert {
        key: value
        for key, value in fs.meta.items()
        if key not in {"last_idle_commit_accepted_at", "updated_at"}
    } == {
        key: value
        for key, value in concurrent.items()
        if key not in {"last_idle_commit_accepted_at", "updated_at"}
    }
    assert len(fs.writes) == 1
    assert fs.events.index("acquire") < fs.events.index("read") < fs.events.index("write")
    assert fs.events[-1] == "release"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_at",
    ["2026-01-01T00:04:00Z", "2026-01-01T08:04:00+08:00"],
)
async def test_record_idle_time_never_moves_existing_acceptance_backwards(
    timeout_service, request_context, existing_at
):
    service, fs = timeout_service
    fs.meta["last_idle_commit_accepted_at"] = existing_at

    await service._record_idle_commit_time("s", request_context, "2026-01-01T00:03:00Z")

    assert parse_iso_datetime(fs.meta["last_idle_commit_accepted_at"]) == parse_iso_datetime(
        existing_at
    )
    assert fs.events[-1] == "release"


@pytest.mark.asyncio
async def test_record_idle_time_deleted_session_is_not_recreated(timeout_service, request_context):
    service, fs = timeout_service
    fs.on_acquire = lambda: setattr(fs, "exists", False)

    await service._record_idle_commit_time("s", request_context, "2026-01-01T00:03:00Z")

    assert fs.exists is False
    assert fs.writes == []
    assert fs.events[-1] == "release"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["read_error", "invalid_json", "not_a_dict", "write_error"])
async def test_record_idle_time_metadata_failures_are_logged_best_effort(
    timeout_service, request_context, service_logs, failure
):
    service, fs = timeout_service
    if failure == "read_error":
        fs.read_error = RuntimeError("metadata read failed")
    elif failure == "invalid_json":
        fs.raw_meta = "{invalid"
    elif failure == "not_a_dict":
        fs.raw_meta = "[]"
    else:
        fs.write_error = RuntimeError("metadata write failed")

    await service._record_idle_commit_time("s", request_context, "2026-01-01T00:03:00Z")

    assert fs.writes == []
    assert fs.events[-1] == "release"
    assert "Failed to record idle commit time" in service_logs.text


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["idle_activity", "idle_cooldown"])
async def test_timeout_commit_deferred_does_not_commit_or_record(
    timeout_service, idle_session, request_context, reason
):
    service, fs = timeout_service
    if reason == "idle_activity":
        idle_session.meta.last_message_at = "2026-01-01T00:01:59Z"
        expected_due_ms = 1767225779000
    else:
        idle_session.meta.last_idle_commit_accepted_at = "2026-01-01T00:01:59Z"
        expected_due_ms = 1767225780000

    assert await service.commit_on_timeout("s", request_context) == {
        "status": "deferred",
        "reason": reason,
        "due_at_ms": expected_due_ms,
        "timeout_seconds": 60,
    }
    idle_session.commit_async.assert_not_awaited()
    assert fs.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["", "invalid timestamp"])
async def test_timeout_commit_invalid_activity_time_fails_closed(
    timeout_service, idle_session, request_context, value
):
    service, fs = timeout_service
    idle_session.meta.last_message_at = value

    with pytest.raises((RuntimeError, ValueError)):
        await service.commit_on_timeout("s", request_context)

    idle_session.commit_async.assert_not_awaited()
    assert fs.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "write"])
async def test_record_idle_time_cancellation_propagates_and_releases_lock(
    timeout_service, request_context, operation
):
    service, fs = timeout_service
    cancellation = asyncio.CancelledError()
    setattr(fs, f"{operation}_error", cancellation)

    with pytest.raises(asyncio.CancelledError) as caught:
        await service._record_idle_commit_time("s", request_context, "2026-01-01T00:03:00Z")

    assert caught.value is cancellation
    assert fs.events[-1] == "release"
    assert fs.locked is False
    assert fs.writes == []


@pytest.mark.asyncio
async def test_record_idle_time_lock_failure_is_logged_best_effort(
    timeout_service, request_context, service_logs
):
    service, fs = timeout_service
    acquire = AsyncMock(spec=fs.acquire, side_effect=RuntimeError("lock unavailable"))
    fs._async_agfs.pathlock_acquire_exact = acquire

    await service._record_idle_commit_time("s", request_context, "2026-01-01T00:03:00Z")

    acquire.assert_awaited_once()
    assert fs.events == []
    assert fs.writes == []
    assert "Failed to record idle commit time" in service_logs.text


@pytest.mark.asyncio
async def test_record_idle_time_replaces_invalid_previous_acceptance(
    timeout_service, request_context, service_logs
):
    service, fs = timeout_service
    fs.meta["last_idle_commit_accepted_at"] = "invalid timestamp"
    accepted_at = "2026-01-01T00:03:00Z"

    await service._record_idle_commit_time("s", request_context, accepted_at)

    assert fs.meta["last_idle_commit_accepted_at"] == accepted_at
    assert len(fs.writes) == 1
    assert fs.events[-1] == "release"
    assert "Replacing invalid idle commit time" in service_logs.text


@pytest.fixture
def enable_default_local_backend(monkeypatch):
    from openviking.service import core

    original_get_config = core.get_openviking_config

    def enabled_config():
        config = original_get_config()
        config.memory.session_auto_commit = SessionAutoCommitConfig(enabled=True)
        return config

    monkeypatch.setattr(core, "get_openviking_config", enabled_config)


@pytest.fixture
def local_service(enable_default_local_backend, service):
    return service


@pytest.mark.asyncio
async def test_default_local_backend_starts_scheduler_without_expiry(local_service):
    assert SessionAutoCommitConfig().idle_backend == "local"
    assert local_service._session_auto_commit_scheduler is not None
    assert local_service._session_auto_commit_scheduler._running is True
    assert local_service._expiry_runtime is None
    assert local_service.sessions._session_expiry_adapter is None


@pytest.fixture
def configure_expiry_without_enabling_redis_backend(monkeypatch):
    from openviking.service import core

    original_get_config = core.get_openviking_config

    def enabled_config():
        config = original_get_config()
        config.memory.session_auto_commit = SessionAutoCommitConfig(enabled=False)
        config.expiry = ExpiryConfig(
            service_name="unused",
            redis={"endpoints": ["127.0.0.1:1"]},
        )
        return config

    monkeypatch.setattr(core, "get_openviking_config", enabled_config)


@pytest.fixture
def expiry_without_redis_backend_service(configure_expiry_without_enabling_redis_backend, service):
    return service


@pytest.mark.asyncio
async def test_expiry_config_without_redis_backend_does_not_start_runtime(
    expiry_without_redis_backend_service,
):
    assert expiry_without_redis_backend_service._expiry_runtime is None
