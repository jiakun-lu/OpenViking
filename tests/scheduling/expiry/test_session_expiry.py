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
from openviking.service.session_expiry import (
    SessionExpiryAdapter,
    SessionTimeoutHandler,
    effective_idle_timeout,
    install_session_expiry,
)
from openviking.session.session import SessionExpirySnapshot


class Scheduler:
    def __init__(self):
        self.reports = []
        self.unregistered = []
        self.handlers = []

    async def report(self, report):
        self.reports.append(report)
        return SimpleNamespace(accepted_due_at_ms=report.due_at_ms)

    async def unregister(self, identity):
        self.unregistered.append(identity)

    def register_handler(self, item_type, handler, *, options=None):
        self.handlers.append((item_type, handler, options))


def test_install_session_expiry_uses_session_defaults():
    scheduler = Scheduler()
    sessions = SimpleNamespace(set_session_expiry_adapter=lambda adapter: None)

    install_session_expiry(scheduler, sessions)

    assert len(scheduler.handlers) == 1
    item_type, _, options = scheduler.handlers[0]
    assert item_type == "session"
    assert options.concurrency == 8
    assert options.metadata_enabled is False


def test_install_session_expiry_uses_configured_concurrency():
    scheduler = Scheduler()
    sessions = SimpleNamespace(set_session_expiry_adapter=lambda adapter: None)

    install_session_expiry(scheduler, sessions, concurrency=32)

    assert scheduler.handlers[0][2].concurrency == 32


def test_adapter_builds_due_from_last_message_and_current_policy():
    scheduler = Scheduler()
    adapter = SessionExpiryAdapter(scheduler)
    snapshot = SessionExpirySnapshot(
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
    adapter = SessionExpiryAdapter(Scheduler())
    snapshot = SessionExpirySnapshot("a", "u", "s", "2026-01-01T00:00:00Z", None)
    assert adapter.build_report(snapshot) is None
    assert effective_idle_timeout({"idle_timeout_seconds": 0}) is None


@pytest.mark.asyncio
async def test_handler_reports_cooldown_before_completing_old_event():
    scheduler = Scheduler()

    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {
                "status": "deferred",
                "reason": "idle_cooldown",
                "due_at_ms": 999,
                "timeout_seconds": 30,
            }

    identity = ItemIdentity("session", "a", "u", "s")
    result = await SessionTimeoutHandler(Sessions(), scheduler).handle(CallbackRequest(identity))
    assert result.status is CallbackStatus.COMPLETED
    assert scheduler.reports[0].due_at_ms == 999


@pytest.mark.asyncio
async def test_handler_maps_not_found_to_completed():
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {"status": "skipped", "reason": "not_found"}

    result = await SessionTimeoutHandler(Sessions(), Scheduler()).handle(
        CallbackRequest(ItemIdentity("session", "a", "u", "s"))
    )
    assert result.status is CallbackStatus.COMPLETED


@pytest.mark.asyncio
async def test_handler_cooldown_report_failure_propagates():
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {
                "status": "deferred",
                "reason": "idle_cooldown",
                "due_at_ms": 999,
                "timeout_seconds": 30,
            }

    scheduler = Scheduler()
    error = RuntimeError("reschedule unavailable")
    scheduler.report = AsyncMock(spec=scheduler.report, side_effect=error)
    identity = ItemIdentity("session", "a", "u", "s")

    with pytest.raises(RuntimeError, match="reschedule unavailable") as caught:
        await SessionTimeoutHandler(Sessions(), scheduler).handle(CallbackRequest(identity))

    assert caught.value is error
    scheduler.report.assert_awaited_once()
    report = scheduler.report.await_args.args[0]
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

    scheduler = Scheduler()
    identity = ItemIdentity("session", "account", "user", "s")

    result = await SessionTimeoutHandler(Sessions(), scheduler).handle(CallbackRequest(identity))

    assert result.status is CallbackStatus.COMPLETED
    assert len(calls) == 1
    session_id, ctx = calls[0]
    assert session_id == "s"
    assert ctx.account_id == "account"
    assert ctx.user.user_id == "user"
    assert scheduler.reports == []


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["idle_disabled", "no_messages"])
async def test_handler_terminal_skips_do_not_reschedule(reason):
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            return {"status": "skipped", "reason": reason}

    scheduler = Scheduler()
    result = await SessionTimeoutHandler(Sessions(), scheduler).handle(
        CallbackRequest(ItemIdentity("session", "a", "u", "s"))
    )

    assert result.status is CallbackStatus.COMPLETED
    assert scheduler.reports == []


@pytest.mark.asyncio
async def test_handler_commit_failure_propagates():
    class Sessions:
        async def commit_on_timeout(self, session_id, ctx):
            raise RuntimeError("source commit failed")

    scheduler = Scheduler()
    with pytest.raises(RuntimeError, match="source commit failed"):
        await SessionTimeoutHandler(Sessions(), scheduler).handle(
            CallbackRequest(ItemIdentity("session", "a", "u", "s"))
        )
    assert scheduler.reports == []


@pytest.mark.asyncio
async def test_adapter_append_reports_current_snapshot_identity_and_policy():
    scheduler = Scheduler()
    snapshot = SessionExpirySnapshot(
        "a", "u", "s", "2026-01-01T00:00:00Z", {"idle_timeout_seconds": 120}
    )

    await SessionExpiryAdapter(scheduler).report_after_append(snapshot)

    assert len(scheduler.reports) == 1
    report = scheduler.reports[0]
    assert report.identity == ItemIdentity("session", "a", "u", "s")
    assert report.due_at_ms == 1767225720000
    assert report.timeout_seconds == 120
    assert report.mode is ExpiryMode.IDLE_ONESHOT


@pytest.fixture
def adapter_logs(caplog):
    from openviking.service.session_expiry import logger

    logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


@pytest.mark.asyncio
async def test_adapter_append_report_failure_is_best_effort(adapter_logs):
    scheduler = Scheduler()
    scheduler.report = AsyncMock(
        spec=scheduler.report, side_effect=RuntimeError("redis unavailable")
    )
    snapshot = SessionExpirySnapshot(
        "a", "u", "s", "2026-01-01T00:00:00Z", {"idle_timeout_seconds": 60}
    )

    await SessionExpiryAdapter(scheduler).report_after_append(snapshot)

    scheduler.report.assert_awaited_once()
    assert "report_failed" in adapter_logs.text


@pytest.mark.asyncio
async def test_adapter_append_report_cancellation_propagates():
    scheduler = Scheduler()
    scheduler.report = AsyncMock(spec=scheduler.report, side_effect=asyncio.CancelledError())
    snapshot = SessionExpirySnapshot(
        "a", "u", "s", "2026-01-01T00:00:00Z", {"idle_timeout_seconds": 60}
    )

    with pytest.raises(asyncio.CancelledError):
        await SessionExpiryAdapter(scheduler).report_after_append(snapshot)
    scheduler.report.assert_awaited_once()
