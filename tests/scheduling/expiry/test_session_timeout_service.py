# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Service-level idle checks and timeout commit delegation."""

import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.scheduling.expiry.config import ExpiryConfig
from openviking.service.session_service import SessionService
from openviking.session import Session
from openviking.session.session import SessionMeta
from openviking_cli.exceptions import NotFoundError
from openviking_cli.utils.config.memory_config import SessionAutoCommitConfig


class MetadataFS:
    """Storage sentinel: timeout delegation must not perform metadata I/O itself."""

    def __init__(self, ctx):
        del ctx
        self.events = []
        self.writes = []


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


def test_timeout_check_empty_messages_precedes_cooldown(idle_session, now):
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


@pytest.mark.parametrize(
    "last_message_at",
    ["", "invalid timestamp", "2026-01-01T00:01:59Z"],
)
def test_timeout_check_ignores_last_message_at(idle_session, now, last_message_at):
    idle_session.meta.last_message_at = last_message_at
    before = copy.deepcopy(idle_session.meta.to_dict())

    assert SessionService._check_timeout_commit(idle_session, now) is None
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


def test_timeout_check_cooldown_uses_current_timeout(idle_session, now):
    idle_session.meta.auto_commit_policy = {"idle_timeout_seconds": 120}
    idle_session.meta.last_idle_commit_accepted_at = "2026-01-01T00:00:30Z"
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
async def test_timeout_commit_delegates_idle_success_recording_to_commit(
    timeout_service, idle_session, request_context, working_memory_enabled
):
    service, fs = timeout_service
    idle_session.meta.memory_policy = (
        None
        if working_memory_enabled is None
        else {"working_memory": {"enabled": working_memory_enabled}}
    )
    result = {"status": "accepted", "archived": True, "task_id": "task", "archive_uri": "archive"}
    idle_session.commit_async.return_value = result

    actual = await service.commit_on_timeout("s", request_context)

    assert actual is result
    service.get.assert_awaited_once_with("s", request_context, auto_create=False)
    idle_session.commit_async.assert_awaited_once_with(
        keep_recent_count=0,
        persist_keep_recent_count=False,
        record_auto_commit_success=True,
        record_idle_commit_success=True,
    )
    assert fs.events == []
    assert fs.writes == []


@pytest.mark.asyncio
async def test_timeout_commit_includes_messages_appended_before_commit_snapshot(
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
        record_idle_commit_success=True,
    )
    assert fs.writes == []


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


@pytest.mark.asyncio
async def test_timeout_commit_cooldown_does_not_commit_or_record(
    timeout_service, idle_session, request_context
):
    service, fs = timeout_service
    idle_session.meta.last_idle_commit_accepted_at = "2026-01-01T00:01:59Z"

    assert await service.commit_on_timeout("s", request_context) == {
        "status": "deferred",
        "reason": "idle_cooldown",
        "due_at_ms": 1767225780000,
        "timeout_seconds": 60,
    }
    idle_session.commit_async.assert_not_awaited()
    assert fs.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["", "invalid timestamp", "2026-01-01T00:01:59Z"])
async def test_timeout_commit_ignores_last_message_at(
    timeout_service, idle_session, request_context, value
):
    service, fs = timeout_service
    result = {"status": "accepted", "archived": True}
    idle_session.meta.last_message_at = value
    idle_session.commit_async.return_value = result

    assert await service.commit_on_timeout("s", request_context) is result
    idle_session.commit_async.assert_awaited_once()
    assert fs.events == []


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
