import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.scheduling.expiry import (
    CallbackRequest,
    CallbackStatus,
    ExpiryMode,
    ItemIdentity,
)
from openviking.service.session_timer import (
    SessionTimeoutHandler,
    SessionTimerAdapter,
    effective_idle_timeout,
)
from openviking.session.session import SessionTimerSnapshot


class Timer:
    def __init__(self):
        self.reports = []
        self.unregistered = []

    async def report(self, report):
        self.reports.append(report)
        return SimpleNamespace(accepted_due_at_ms=report.due_at_ms)

    async def unregister(self, identity):
        self.unregistered.append(identity)


def test_adapter_builds_due_from_last_message_and_current_policy():
    timer = Timer()
    adapter = SessionTimerAdapter(timer)
    snapshot = SessionTimerSnapshot(
        "a",
        "u",
        "s",
        "2026-01-01T00:00:00+00:00",
        {"idle_timeout_seconds": 60},
    )
    report = adapter.build_report(snapshot)
    assert report is not None
    assert report.due_at_ms == 1767225660000
    assert report.timeout_seconds == 60


def test_disabled_policy_produces_no_report():
    adapter = SessionTimerAdapter(Timer())
    snapshot = SessionTimerSnapshot("a", "u", "s", "2026-01-01T00:00:00Z", None)
    assert adapter.build_report(snapshot) is None
    assert effective_idle_timeout({"idle_timeout_seconds": 0}) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["idle_cooldown", "idle_activity"])
async def test_handler_reports_deferred_before_completing_old_event(reason):
    timer = Timer()

    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {
                "status": "deferred",
                "reason": reason,
                "due_at_ms": 999,
                "timeout_seconds": 30,
            }

    identity = ItemIdentity("session", "a", "u", "s")
    result = await SessionTimeoutHandler(Sessions(), timer).handle(CallbackRequest(identity))
    assert result.status is CallbackStatus.COMPLETED
    assert timer.reports[0].due_at_ms == 999


@pytest.mark.asyncio
async def test_handler_maps_not_found_to_completed():
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {"status": "skipped", "reason": "not_found"}

    result = await SessionTimeoutHandler(Sessions(), Timer()).handle(
        CallbackRequest(ItemIdentity("session", "a", "u", "s"))
    )
    assert result.status is CallbackStatus.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["idle_cooldown", "idle_activity"])
async def test_handler_deferred_report_failure_propagates(reason):
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {
                "status": "deferred",
                "reason": reason,
                "due_at_ms": 999,
                "timeout_seconds": 30,
            }

    timer = Timer()
    error = RuntimeError("reschedule unavailable")
    timer.report = AsyncMock(spec=timer.report, side_effect=error)
    identity = ItemIdentity("session", "a", "u", "s")

    with pytest.raises(RuntimeError, match="reschedule unavailable") as caught:
        await SessionTimeoutHandler(Sessions(), timer).handle(CallbackRequest(identity))

    assert caught.value is error
    timer.report.assert_awaited_once()
    report = timer.report.await_args.args[0]
    assert report.identity == identity
    assert report.due_at_ms == 999
    assert report.timeout_seconds == 30
    assert report.mode is ExpiryMode.IDLE_ONESHOT


@pytest.mark.asyncio
async def test_handler_reads_source_session_from_identity_without_metadata():
    calls = []

    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            calls.append((session_id, ctx))
            return {"status": "accepted", "task_id": "task"}

    timer = Timer()
    identity = ItemIdentity("session", "account", "user", "s")

    result = await SessionTimeoutHandler(Sessions(), timer).handle(CallbackRequest(identity))

    assert result.status is CallbackStatus.COMPLETED
    assert len(calls) == 1
    session_id, ctx = calls[0]
    assert session_id == "s"
    assert ctx.account_id == "account"
    assert ctx.user.user_id == "user"
    assert timer.reports == []


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["idle_disabled", "no_messages"])
async def test_handler_terminal_skips_do_not_reschedule(reason):
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {"status": "skipped", "reason": reason}

    timer = Timer()
    result = await SessionTimeoutHandler(Sessions(), timer).handle(
        CallbackRequest(ItemIdentity("session", "a", "u", "s"))
    )

    assert result.status is CallbackStatus.COMPLETED
    assert timer.reports == []


@pytest.mark.asyncio
async def test_handler_commit_failure_propagates():
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            raise RuntimeError("source commit failed")

    timer = Timer()
    with pytest.raises(RuntimeError, match="source commit failed"):
        await SessionTimeoutHandler(Sessions(), timer).handle(
            CallbackRequest(ItemIdentity("session", "a", "u", "s"))
        )
    assert timer.reports == []


@pytest.mark.asyncio
async def test_adapter_append_reports_current_snapshot_identity_and_policy():
    timer = Timer()
    snapshot = SessionTimerSnapshot(
        "a", "u", "s", "2026-01-01T00:00:00Z", {"idle_timeout_seconds": 120}
    )

    await SessionTimerAdapter(timer).report_after_append(snapshot)

    assert len(timer.reports) == 1
    report = timer.reports[0]
    assert report.identity == ItemIdentity("session", "a", "u", "s")
    assert report.due_at_ms == 1767225720000
    assert report.timeout_seconds == 120
    assert report.mode is ExpiryMode.IDLE_ONESHOT


@pytest.fixture
def adapter_logs(caplog):
    from openviking.service.session_timer import logger

    logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


@pytest.mark.asyncio
async def test_adapter_append_report_failure_is_best_effort(adapter_logs):
    timer = Timer()
    timer.report = AsyncMock(spec=timer.report, side_effect=RuntimeError("redis unavailable"))
    snapshot = SessionTimerSnapshot(
        "a", "u", "s", "2026-01-01T00:00:00Z", {"idle_timeout_seconds": 60}
    )

    await SessionTimerAdapter(timer).report_after_append(snapshot)

    timer.report.assert_awaited_once()
    assert "report_failed" in adapter_logs.text


@pytest.mark.asyncio
async def test_adapter_append_report_cancellation_propagates():
    timer = Timer()
    timer.report = AsyncMock(spec=timer.report, side_effect=asyncio.CancelledError())
    snapshot = SessionTimerSnapshot(
        "a", "u", "s", "2026-01-01T00:00:00Z", {"idle_timeout_seconds": 60}
    )

    with pytest.raises(asyncio.CancelledError):
        await SessionTimerAdapter(timer).report_after_append(snapshot)
    timer.report.assert_awaited_once()
