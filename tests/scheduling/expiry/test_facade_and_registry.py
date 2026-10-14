import pytest

from openviking.scheduling.expiry import (
    CallbackResult,
    CallbackStatus,
    ExpiryError,
    ExpiryRegistration,
    ExpiryRegistrationResult,
    HandlerOptions,
    ItemIdentity,
)
from openviking.scheduling.expiry.metrics import ExpiryMetrics
from openviking.scheduling.expiry.models import StoreOperationError
from openviking.scheduling.expiry.registry import HandlerRegistry
from openviking.scheduling.expiry.service import ExpiryRuntimeState, _ExpirySchedulerFacade


class Handler:
    async def handle(self, request):
        return CallbackResult(CallbackStatus.COMPLETED, "ok")


class Store:
    def __init__(self):
        self.calls = []
        self.fail_metadata = False

    async def upsert_resource(self, shard_id, item_id, task):
        self.calls.append("resource")
        return ExpiryRegistrationResult(task.due_at_ms)

    async def ensure_metadata(self, item_id, task):
        self.calls.append("metadata")
        if self.fail_metadata:
            raise StoreOperationError("redis_error", "metadata failed", retryable=True)

    async def unregister_resource(self, shard_id, item_id, *, delete_metadata=True):
        self.calls.append(("unregister", delete_metadata))


def test_registry_validation_and_freeze():
    registry = HandlerRegistry()
    registry.register("session", Handler(), HandlerOptions(concurrency=2, max_attempts=3))
    bindings = registry.freeze()
    assert bindings["session"].options.max_attempts == 3
    with pytest.raises(ExpiryError) as exc:
        registry.register("watch", Handler(), HandlerOptions())
    assert exc.value.code == "registration_closed"


def test_registry_rejects_unsupported_type():
    registry = HandlerRegistry()
    with pytest.raises(ExpiryError) as exc:
        registry.register("watch", Handler(), HandlerOptions())
    assert exc.value.code == "unregistered_type"


@pytest.mark.asyncio
async def test_report_writes_resource_before_metadata():
    store = Store()
    state = ExpiryRuntimeState(storage_ready=True, accepting_calls=True)
    facade = _ExpirySchedulerFacade(
        store,
        HandlerRegistry(),
        state,
        shard_count=8,
        reporting_enabled=True,
        metrics=ExpiryMetrics(),
    )
    result = await facade.report(
        ExpiryRegistration(ItemIdentity("session", "a", "u", "s"), 1234, 60)
    )
    assert result.accepted_due_at_ms == 1234
    assert store.calls == ["resource", "metadata"]


@pytest.mark.asyncio
async def test_report_skips_metadata_when_handler_disables_it():
    store = Store()
    registry = HandlerRegistry()
    registry.register("session", Handler(), HandlerOptions(metadata_enabled=False))
    facade = _ExpirySchedulerFacade(
        store,
        registry,
        ExpiryRuntimeState(storage_ready=True, accepting_calls=True),
        shard_count=8,
        reporting_enabled=True,
        metrics=ExpiryMetrics(),
    )

    result = await facade.report(
        ExpiryRegistration(ItemIdentity("session", "a", "u", "s"), 1234, 60)
    )

    assert result.accepted_due_at_ms == 1234
    assert store.calls == ["resource"]


@pytest.mark.asyncio
async def test_metadata_failure_does_not_roll_back_resource():
    store = Store()
    store.fail_metadata = True
    facade = _ExpirySchedulerFacade(
        store,
        HandlerRegistry(),
        ExpiryRuntimeState(storage_ready=True, accepting_calls=True),
        shard_count=8,
        reporting_enabled=True,
        metrics=ExpiryMetrics(),
    )
    with pytest.raises(ExpiryError) as exc:
        await facade.report(ExpiryRegistration(ItemIdentity("session", "a", "u", "s"), 1234, 60))
    assert exc.value.code == "timer_unavailable"
    assert store.calls == ["resource", "metadata"]


@pytest.mark.asyncio
async def test_report_rejects_unsupported_type_before_store_write():
    store = Store()
    state = ExpiryRuntimeState(storage_ready=True, accepting_calls=True)
    facade = _ExpirySchedulerFacade(
        store,
        HandlerRegistry(),
        state,
        shard_count=8,
        reporting_enabled=True,
        metrics=ExpiryMetrics(),
    )
    report = ExpiryRegistration(ItemIdentity("watch", "a", "u", "s"), 1234, 60)
    with pytest.raises(ExpiryError) as exc:
        await facade.report(report)
    assert exc.value.code == "unregistered_type"
    assert store.calls == []


@pytest.mark.asyncio
async def test_unregister_rejects_unsupported_type_before_store_write():
    store = Store()
    facade = _ExpirySchedulerFacade(
        store,
        HandlerRegistry(),
        ExpiryRuntimeState(storage_ready=True, accepting_calls=True),
        shard_count=8,
        reporting_enabled=True,
        metrics=ExpiryMetrics(),
    )
    with pytest.raises(ExpiryError) as exc:
        await facade.unregister(ItemIdentity("watch", "a", "u", "s"))
    assert exc.value.code == "unregistered_type"
    assert store.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata_enabled", "delete_metadata"),
    [(True, True), (False, False)],
)
async def test_unregister_follows_handler_metadata_option(metadata_enabled, delete_metadata):
    store = Store()
    registry = HandlerRegistry()
    registry.register(
        "session",
        Handler(),
        HandlerOptions(metadata_enabled=metadata_enabled),
    )
    facade = _ExpirySchedulerFacade(
        store,
        registry,
        ExpiryRuntimeState(storage_ready=True, accepting_calls=True),
        shard_count=8,
        reporting_enabled=True,
        metrics=ExpiryMetrics(),
    )

    await facade.unregister(ItemIdentity("session", "a", "u", "s"))

    assert store.calls == [("unregister", delete_metadata)]
