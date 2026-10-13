# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Session business adapter for the generic distributed resource timer."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from openviking.scheduling.expiry import (
    SESSION_ITEM_TYPE,
    CallbackRequest,
    CallbackResult,
    CallbackStatus,
    ExpiryMode,
    ExpiryRegistration,
    ExpiryScheduler,
    HandlerOptions,
    ItemIdentity,
)
from openviking.server.identity import RequestContext, Role
from openviking.session.auto_commit_policy import AutoCommitPolicy
from openviking.session.session import SessionTimerSnapshot
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

if TYPE_CHECKING:
    from openviking.service.session_service import SessionService

logger = get_logger(__name__)


def install_session_timer(
    timer: ExpiryScheduler,
    sessions: "SessionService",
    *,
    reporting_enabled: bool,
) -> None:
    timer.register_handler(
        SESSION_ITEM_TYPE,
        SessionTimeoutHandler(sessions, timer),
        options=HandlerOptions(concurrency=4, max_attempts=3),
    )
    sessions.set_session_timer_adapter(SessionTimerAdapter(timer) if reporting_enabled else None)


def effective_idle_timeout(policy: dict | None) -> int | None:
    if policy is None:
        return None
    timeout = AutoCommitPolicy.from_dict(policy).idle_timeout_seconds
    return timeout if timeout > 0 else None


def timestamp_ms(value: str) -> int:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


class SessionTimerAdapter:
    def __init__(self, timer: ExpiryScheduler) -> None:
        self._timer = timer

    def build_report(self, snapshot: SessionTimerSnapshot) -> ExpiryRegistration | None:
        timeout = effective_idle_timeout(snapshot.auto_commit_policy)
        if timeout is None:
            return None
        return ExpiryRegistration(
            identity=ItemIdentity(
                SESSION_ITEM_TYPE,
                snapshot.account_id,
                snapshot.user_id,
                snapshot.session_id,
            ),
            due_at_ms=timestamp_ms(snapshot.last_message_at) + timeout * 1000,
            timeout_seconds=timeout,
            mode=ExpiryMode.IDLE_ONESHOT,
        )

    async def report_after_append(self, snapshot: SessionTimerSnapshot) -> None:
        try:
            report = self.build_report(snapshot)
            if report is not None:
                await self._timer.report(report)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.record_report_failure("report_failed", exc)

    async def unregister_after_delete(self, identity: ItemIdentity) -> None:
        try:
            await self._timer.unregister(identity)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.record_report_failure("unregister_failed", exc)

    def record_report_failure(self, reason: str, exc: Exception | None = None) -> None:
        logger.warning(
            "Session timer operation failed reason=%s error=%s",
            reason,
            type(exc).__name__ if exc is not None else "unknown",
            exc_info=exc is not None,
        )


class SessionTimeoutHandler:
    def __init__(self, sessions: "SessionService", timer: ExpiryScheduler) -> None:
        self._sessions = sessions
        self._timer = timer

    async def handle(self, request: CallbackRequest) -> CallbackResult:
        result = await self._sessions.commit_on_timeout(
            request.identity.local_id, self._make_context(request.identity)
        )
        if result.get("status") == "deferred":
            await self._report_deferred(request.identity, result)
            return CallbackResult(CallbackStatus.COMPLETED, "idle_rescheduled")
        return self._to_callback_result(result)

    async def _report_deferred(self, identity: ItemIdentity, result: dict[str, Any]) -> None:
        if result.get("reason") not in {"idle_cooldown", "idle_activity"}:
            raise ValueError("Unexpected deferred session result")
        await self._timer.report(
            ExpiryRegistration(
                identity=identity,
                due_at_ms=result["due_at_ms"],
                timeout_seconds=result["timeout_seconds"],
                mode=ExpiryMode.IDLE_ONESHOT,
            )
        )

    def _make_context(self, identity: ItemIdentity) -> RequestContext:
        return RequestContext(
            user=UserIdentifier(account_id=identity.account_id, user_id=identity.user_id),
            role=Role.USER,
        )

    def _to_callback_result(self, result: dict[str, Any]) -> CallbackResult:
        status = result.get("status")
        if status == "accepted":
            return CallbackResult(CallbackStatus.COMPLETED, "accepted")
        if status == "skipped" and result.get("reason") in {
            "not_found",
            "idle_disabled",
            "no_messages",
        }:
            return CallbackResult(CallbackStatus.COMPLETED, str(result["reason"]))
        return CallbackResult(CallbackStatus.RETRY, "unexpected_commit_result")
