import asyncio
import gc
import time
import weakref
from types import SimpleNamespace

import pytest

from openviking.scheduling.expiry import leases as leases_module
from openviking.scheduling.expiry import models as models_module
from openviking.scheduling.expiry.config import ExpiryConfig, ExpiryRedisConfig
from openviking.scheduling.expiry.leases import ShardLeaseCoordinator
from openviking.scheduling.expiry.metrics import ExpiryMetrics
from openviking.scheduling.expiry.models import (
    OwnerRole,
    OwnerToken,
    ShardLease,
    StoreOperationError,
)
from openviking.scheduling.expiry.shard_runner import ShardExecution, ShardRunner


class Store:
    def __init__(self, *, busy=False, renew_outcome="success", block_renew=False):
        self.busy = busy
        self.renew_outcome = renew_outcome
        self.block_renew = block_renew
        self.renew_started = asyncio.Event()
        self.allow_renew = asyncio.Event()
        self.acquired = []
        self.renewed = []
        self.released = []

    async def try_acquire_owner(self, role, shard_id, token, ttl_seconds, *, item_type=None):
        self.acquired.append((role, item_type, shard_id, token, ttl_seconds))
        if self.busy:
            return None
        return OwnerToken(role, shard_id, token, 9999999999999, item_type)

    async def renew_owner(self, owner, ttl_seconds):
        self.renewed.append((owner, ttl_seconds))
        self.renew_started.set()
        if self.block_renew:
            await self.allow_renew.wait()
        if self.renew_outcome == "error":
            raise RuntimeError("renew failed")
        if self.renew_outcome == "lost":
            return None
        return OwnerToken(
            owner.role,
            owner.shard_id,
            owner.token,
            owner.expires_at_ms + 1,
            owner.item_type,
        )

    async def release_owner(self, owner):
        self.released.append(owner)
        return True


def lease():
    return ShardLease(
        OwnerToken(OwnerRole.SCHEDULER, 0, "token", 9999999999999),
        asyncio.Event(),
        float("inf"),
    )


def config(**overrides):
    values = {
        "shard_count": 1,
        "owner_renew_interval_seconds": 0.01,
        "shutdown_timeout_seconds": 1.0,
        "redis": ExpiryRedisConfig.model_construct(request_budget_seconds=0.05),
    }
    values.update(overrides)
    return ExpiryConfig.model_construct(**values)


def runner(store, timer_config=None):
    timer_config = timer_config or config()
    metrics = ExpiryMetrics()
    coordinator = ShardLeaseCoordinator(store, timer_config, metrics)
    return ShardRunner(coordinator, timer_config, metrics)


@pytest.mark.asyncio
async def test_coordinator_acquires_renews_and_releases_matching_owner():
    store = Store()
    coordinator = ShardLeaseCoordinator(store, config(), ExpiryMetrics())
    acquired = await coordinator.try_acquire(OwnerRole.CALLBACK, 3, item_type="session")
    assert acquired is not None
    assert acquired.owner.role is OwnerRole.CALLBACK
    assert acquired.owner.item_type == "session"
    assert acquired.owner.shard_id == 3
    assert len(acquired.owner.token) == 32
    int(acquired.owner.token, 16)
    assert acquired.valid_until_monotonic > time.monotonic()
    original = acquired.owner
    assert await coordinator.renew(acquired) is True
    assert acquired.owner != original
    assert acquired.owner.token == original.token
    await coordinator.release(acquired)
    assert store.released == [acquired.owner]


@pytest.mark.asyncio
async def test_coordinator_returns_none_when_shard_is_busy():
    store = Store(busy=True)
    coordinator = ShardLeaseCoordinator(store, config(), ExpiryMetrics())
    assert await coordinator.try_acquire(OwnerRole.SCHEDULER, 0) is None
    assert store.renewed == []
    assert store.released == []


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["lost", "error"])
async def test_failed_renewal_marks_lease_lost_and_cannot_revive(outcome):
    store = Store(renew_outcome=outcome)
    coordinator = ShardLeaseCoordinator(store, config(), ExpiryMetrics())
    acquired = await coordinator.try_acquire(OwnerRole.SCHEDULER, 0)
    original = acquired.owner
    assert await coordinator.renew(acquired) is False
    assert acquired.lost.is_set()
    store.renew_outcome = "success"
    assert await coordinator.renew(acquired) is False
    await coordinator.release(acquired)
    assert acquired.owner == original
    assert len(store.renewed) == 1
    assert store.released == []


@pytest.mark.asyncio
@pytest.mark.parametrize("invalidation", ["deadline", "lost", "revoked"])
async def test_successful_renew_response_cannot_revive_invalidated_lease(monkeypatch, invalidation):
    clock = SimpleNamespace(now=10.0)
    local_time = SimpleNamespace(monotonic=lambda: clock.now)
    monkeypatch.setattr(leases_module, "time", local_time)
    monkeypatch.setattr(models_module, "time", local_time)
    store = Store(block_renew=True)
    coordinator = ShardLeaseCoordinator(store, config(), ExpiryMetrics())
    acquired = await coordinator.try_acquire(OwnerRole.SCHEDULER, 0)
    original = acquired.owner
    original_deadline = acquired.valid_until_monotonic
    renew_task = asyncio.create_task(coordinator.renew(acquired))
    try:
        await asyncio.wait_for(store.renew_started.wait(), timeout=2.0)
        if invalidation == "deadline":
            clock.now = original_deadline
        else:
            getattr(acquired, invalidation).set()
        store.allow_renew.set()
        assert await asyncio.wait_for(renew_task, timeout=2.0) is False
        assert acquired.owner == original
        assert acquired.valid_until_monotonic == original_deadline
        with pytest.raises(StoreOperationError, match="no longer valid"):
            acquired.ensure_active()
    finally:
        store.allow_renew.set()
        await asyncio.wait_for(asyncio.gather(renew_task, return_exceptions=True), timeout=2.0)


@pytest.mark.asyncio
async def test_release_does_not_delete_expired_owner():
    store = Store()
    coordinator = ShardLeaseCoordinator(store, config(), ExpiryMetrics())
    acquired = lease()
    acquired.valid_until_monotonic = 0.0
    await coordinator.release(acquired)
    assert acquired.lost.is_set()
    assert store.released == []


@pytest.mark.asyncio
async def test_runner_releases_normally_completed_processor_and_children():
    store = Store()
    shard_runner = runner(store)
    observed = []

    async def process(execution, stop):
        execution.ensure_active()
        observed.append((execution.owner, execution.shard_id))

        async def child():
            execution.ensure_active()
            observed.append("child")

        await execution.spawn(child)

    assert (
        await asyncio.wait_for(
            shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, process, asyncio.Event()),
            timeout=2.0,
        )
        is True
    )
    assert observed[0][1] == 0
    assert observed[1] == "child"
    assert store.released == [observed[0][0]]
    assert shard_runner.has_pending is False


@pytest.mark.asyncio
async def test_runner_busy_shard_does_not_start_processor():
    store = Store(busy=True)
    shard_runner = runner(store)
    called = []

    async def process(execution, stop):
        called.append(execution)

    assert (
        await shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, process, asyncio.Event())
        is False
    )
    assert called == []
    assert store.released == []
    assert shard_runner.has_pending is False


@pytest.mark.asyncio
async def test_callback_runner_scopes_active_work_by_item_type():
    store = Store()
    shard_runner = runner(store)
    session_started = asyncio.Event()
    release_session = asyncio.Event()

    async def session_process(execution, stop):
        session_started.set()
        await release_session.wait()
        execution.ensure_active()

    async def watch_process(execution, stop):
        execution.ensure_active()

    session_task = asyncio.create_task(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            session_process,
            asyncio.Event(),
            item_type="session",
        )
    )
    try:
        await asyncio.wait_for(session_started.wait(), timeout=2.0)
        assert (
            await shard_runner.run_if_acquired(
                OwnerRole.CALLBACK,
                0,
                watch_process,
                asyncio.Event(),
                item_type="watch",
            )
            is True
        )
    finally:
        release_session.set()
        await asyncio.wait_for(session_task, timeout=2.0)


@pytest.mark.asyncio
async def test_runner_does_not_acquire_after_stop():
    store = Store()
    shard_runner = runner(store)
    stop = asyncio.Event()
    stop.set()

    async def process(execution, stop):
        pytest.fail("A stopped runner must not invoke the processor")

    assert await shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, process, stop) is False
    assert store.acquired == []


@pytest.mark.asyncio
async def test_stop_grace_allows_processor_to_finish_and_release():
    store = Store()
    shard_runner = runner(store)
    stop = asyncio.Event()
    started = asyncio.Event()
    observed = []

    async def process(execution, current_stop):
        started.set()
        await current_stop.wait()
        execution.ensure_active()
        observed.append(execution.owner)

    run_task = asyncio.create_task(
        shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, process, stop)
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)
        stop.set()
        assert await asyncio.wait_for(run_task, timeout=2.0) is True
        assert store.released == observed
        assert shard_runner.has_pending is False
    finally:
        stop.set()
        await asyncio.wait_for(asyncio.gather(run_task, return_exceptions=True), timeout=2.0)


@pytest.mark.asyncio
async def test_renewal_loss_cancels_processor_during_stop_grace():
    store = Store(renew_outcome="lost", block_renew=True)
    shard_runner = runner(store)
    stop = asyncio.Event()
    process_started = asyncio.Event()
    stop_observed = asyncio.Event()
    process_cancelled = asyncio.Event()
    release_process = asyncio.Event()

    async def process(execution, current_stop):
        process_started.set()
        try:
            await current_stop.wait()
            stop_observed.set()
            await release_process.wait()
        except asyncio.CancelledError:
            process_cancelled.set()
            raise

    run_task = asyncio.create_task(
        shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, process, stop)
    )
    cancelled_task = asyncio.create_task(process_cancelled.wait())
    try:
        await asyncio.wait_for(process_started.wait(), timeout=2.0)
        await asyncio.wait_for(store.renew_started.wait(), timeout=2.0)
        stop.set()
        await asyncio.wait_for(stop_observed.wait(), timeout=2.0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        store.allow_renew.set()
        finished, _ = await asyncio.wait({cancelled_task}, timeout=0.3)
        assert cancelled_task in finished, "Lease loss must not wait for the stop grace to expire"
        await asyncio.wait_for(run_task, timeout=2.0)
        assert store.released == []
        assert shard_runner.has_pending is False
    finally:
        stop.set()
        store.allow_renew.set()
        release_process.set()
        cancelled_task.cancel()
        await asyncio.gather(cancelled_task, return_exceptions=True)
        await asyncio.wait_for(asyncio.gather(run_task, return_exceptions=True), timeout=2.0)


@pytest.mark.asyncio
async def test_processor_exception_releases_active_lease():
    store = Store()
    shard_runner = runner(store)
    owners = []

    async def process(execution, stop):
        owners.append(execution.owner)
        raise RuntimeError("processor failed")

    with pytest.raises(RuntimeError, match="processor failed"):
        await asyncio.wait_for(
            shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, process, asyncio.Event()),
            timeout=2.0,
        )
    assert store.released == owners
    assert shard_runner.has_pending is False


@pytest.mark.asyncio
async def test_cancellation_resistant_processor_is_bounded_retained_and_blocks_same_role():
    store = Store()
    shard_runner = runner(store, config(shutdown_timeout_seconds=0.02))
    stop = asyncio.Event()
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release_process = asyncio.Event()
    process_refs = []

    async def process(execution, current_stop):
        process_refs.append(weakref.ref(asyncio.current_task()))
        started.set()
        while not release_process.is_set():
            try:
                await release_process.wait()
            except asyncio.CancelledError:
                cancelled.set()

    async def completed(execution, current_stop):
        execution.ensure_active()

    run_task = asyncio.create_task(
        shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, process, stop)
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)
        stop.set()
        await asyncio.wait_for(cancelled.wait(), timeout=2.0)
        finished, _ = await asyncio.wait({run_task}, timeout=0.3)
        assert run_task in finished, (
            "Cleanup must not wait indefinitely for cancellation resistance"
        )
        with pytest.raises(StoreOperationError) as error:
            run_task.result()
        assert error.value.code == "shard_not_stopped"
        assert shard_runner.has_pending is True
        gc.collect()
        assert process_refs[0]() is not None
        assert not process_refs[0]().done()
        assert await asyncio.wait_for(shard_runner.wait_pending(0.01), timeout=2.0) is False
        acquisition_count = len(store.acquired)
        with pytest.raises(StoreOperationError) as error:
            await shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 1, completed, asyncio.Event())
        assert error.value.code == "shard_not_stopped"
        assert len(store.acquired) == acquisition_count
        assert store.released == []
        # Pending work blocks only its own role.
        assert (
            await shard_runner.run_if_acquired(
                OwnerRole.CALLBACK,
                0,
                completed,
                asyncio.Event(),
                item_type="session",
            )
            is True
        )
        release_process.set()
        assert await asyncio.wait_for(shard_runner.wait_pending(1.0), timeout=2.0) is True
        assert shard_runner.has_pending is False
        assert (
            await shard_runner.run_if_acquired(OwnerRole.SCHEDULER, 0, completed, asyncio.Event())
            is True
        )
        assert len(store.released) == 2
    finally:
        stop.set()
        release_process.set()
        await asyncio.wait_for(asyncio.gather(run_task, return_exceptions=True), timeout=2.0)
        assert await asyncio.wait_for(shard_runner.wait_pending(1.0), timeout=2.0) is True


@pytest.mark.asyncio
async def test_lease_loss_cancels_spawned_child_and_prevents_late_ack():
    store = Store(renew_outcome="lost", block_renew=True)
    shard_runner = runner(store)
    child_started = asyncio.Event()
    child_cancelled = asyncio.Event()
    child_finished = asyncio.Event()
    release_child = asyncio.Event()
    acked = []
    executions = []

    async def process(execution, stop):
        executions.append(execution)

        async def child():
            child_started.set()
            try:
                while not release_child.is_set():
                    try:
                        await release_child.wait()
                    except asyncio.CancelledError:
                        child_cancelled.set()
                execution.ensure_active()
                acked.append("ack")
            finally:
                child_finished.set()

        execution.spawn(child)
        await execution.wait(60.0, stop)

    run_task = asyncio.create_task(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            process,
            asyncio.Event(),
            item_type="session",
        )
    )
    try:
        await asyncio.wait_for(child_started.wait(), timeout=2.0)
        await asyncio.wait_for(store.renew_started.wait(), timeout=2.0)
        store.allow_renew.set()
        await asyncio.wait_for(child_cancelled.wait(), timeout=2.0)
        finished, _ = await asyncio.wait({run_task}, timeout=0.3)
        assert run_task in finished
        with pytest.raises(StoreOperationError) as error:
            run_task.result()
        assert error.value.code == "shard_not_stopped"
        assert shard_runner.has_pending is True
        assert store.released == []
        called = []

        async def forbidden_child():
            called.append(True)

        with pytest.raises(StoreOperationError):
            executions[0].spawn(forbidden_child)
        assert called == []
        release_child.set()
        await asyncio.wait_for(child_finished.wait(), timeout=2.0)
        assert await shard_runner.wait_pending(1.0) is True
        assert acked == []
        assert store.released == []
        assert shard_runner.has_pending is False
    finally:
        store.allow_renew.set()
        release_child.set()
        await asyncio.wait_for(asyncio.gather(run_task, return_exceptions=True), timeout=2.0)
        assert await asyncio.wait_for(shard_runner.wait_pending(1.0), timeout=2.0) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("invalidation", ["lost", "revoked"])
async def test_execution_wait_wakes_and_rechecks_invalidation(invalidation):
    acquired = lease()
    execution = ShardExecution(acquired)
    wait_task = asyncio.create_task(execution.wait(60.0, asyncio.Event()))
    try:
        await asyncio.sleep(0)
        getattr(acquired, invalidation).set()
        with pytest.raises(StoreOperationError):
            await asyncio.wait_for(wait_task, timeout=2.0)
    finally:
        getattr(acquired, invalidation).set()
        await asyncio.wait_for(asyncio.gather(wait_task, return_exceptions=True), timeout=2.0)


@pytest.mark.asyncio
async def test_execution_wait_rechecks_local_deadline_without_renewal():
    acquired = lease()
    acquired.valid_until_monotonic = time.monotonic() + 0.03
    execution = ShardExecution(acquired)
    with pytest.raises(StoreOperationError):
        await asyncio.wait_for(execution.wait(60.0, asyncio.Event()), timeout=2.0)
    assert acquired.lost.is_set()
