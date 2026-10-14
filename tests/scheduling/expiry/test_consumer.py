import asyncio

import pytest

from openviking.scheduling.expiry import consumer as consumer_module
from openviking.scheduling.expiry.api import (
    CallbackResult,
    CallbackStatus,
    HandlerOptions,
    ItemIdentity,
)
from openviking.scheduling.expiry.config import ExpiryConfig, ExpiryRedisConfig
from openviking.scheduling.expiry.consumer import ExpiryConsumer
from openviking.scheduling.expiry.keys import encode_item_id
from openviking.scheduling.expiry.leases import ShardLeaseCoordinator
from openviking.scheduling.expiry.metrics import ExpiryMetrics
from openviking.scheduling.expiry.models import (
    BucketItem,
    BucketPage,
    BucketRef,
    Claim,
    OwnerRole,
    OwnerToken,
    RegistryPage,
    StoreOperationError,
    claim_layer_bounds,
    decode_claim_score,
    encode_claim_score,
    encode_ready_claim_score,
)
from openviking.scheduling.expiry.registry import HandlerBinding
from openviking.scheduling.expiry.shard_runner import ShardRunner


class Store:
    def __init__(
        self,
        *,
        empty=False,
        attempt=0,
        now_ms=200_000,
        lose_on_renew=False,
    ):
        self.identity = ItemIdentity("session", "account", "user", "session-id")
        self.bucket = BucketRef("session", 0, 60_000)
        self.item_id = encode_item_id(self.identity)
        self.score = None if empty else encode_claim_score(attempt, 123)
        self.now_ms = now_ms
        self.empty = empty
        self.lose_on_renew = lose_on_renew
        self.claim_requests = []
        self.claims = []
        self.advanced = []
        self.acked = []
        self.released = []
        self.metadata_reads = []
        self.renew_started = asyncio.Event()
        self.allow_renew = asyncio.Event()
        self.callback_finished = asyncio.Event()
        self.deferred = []

    async def try_acquire_owner(self, role, shard_id, token, ttl_seconds, *, item_type=None):
        return OwnerToken(role, shard_id, token, 9999999999999, item_type)

    async def renew_owner(self, owner, ttl_seconds):
        self.renew_started.set()
        if self.lose_on_renew:
            await self.allow_renew.wait()
            return None
        return owner

    async def release_owner(self, owner):
        self.released.append(owner)
        return True

    async def read_registry_page(self, shard_id, *, after, limit):
        return RegistryPage(() if self.score is None else (self.bucket,), None)

    async def read_time_ms(self):
        return self.now_ms

    async def read_bucket_page(self, bucket, *, attempt, limit):
        if self.score is None:
            return BucketPage(())
        lower, upper = claim_layer_bounds(attempt)
        if lower <= self.score <= upper:
            return BucketPage((BucketItem(self.item_id, self.score),))
        return BucketPage(())

    async def claim(self, bucket, item, *, attempt, started_at_seconds):
        self.claim_requests.append(
            (bucket, item, {"attempt": attempt, "started_at_seconds": started_at_seconds})
        )
        if self.score != item.score:
            return None
        self.score = encode_claim_score(attempt, started_at_seconds)
        claim = Claim(bucket, item.item_id, self.score)
        self.claims.append(claim)
        return claim

    async def advance_retry(self, claim):
        if self.score != claim.claim_score:
            return False
        attempt, _ = decode_claim_score(claim.claim_score)
        self.score = encode_ready_claim_score(attempt + 1)
        self.advanced.append(claim)
        return True

    async def read_metadata(self, item_id):
        self.metadata_reads.append(item_id)
        return None

    async def ack(self, claim):
        if self.score != claim.claim_score:
            return False
        self.score = None
        self.acked.append(claim)
        return True

    async def remove_registration_if_empty(self, bucket):
        return self.score is None

    async def defer_registration(self, owner, bucket, *, next_poll_at_ms):
        self.deferred.append((owner, bucket, next_poll_at_ms))
        return True


class Handler:
    def __init__(self, *, status=CallbackStatus.COMPLETED, error=None, resist_cancellation=False):
        self.status = status
        self.error = error
        self.resist_cancellation = resist_cancellation
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()
        self.requests = []

    async def handle(self, request):
        self.requests.append(request)
        self.started.set()
        try:
            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    self.cancelled.set()
                    if not self.resist_cancellation:
                        raise
            if self.error is not None:
                raise self.error
            return CallbackResult(self.status, "test callback")
        finally:
            self.finished.set()


class Runner:
    def __init__(self, *, stop_on_shard=0, error=None):
        self.calls = []
        self.stop_on_shard = stop_on_shard
        self.error = error

    async def run_if_acquired(self, role, shard_id, process, stop, *, item_type=None):
        self.calls.append((role, item_type, shard_id, process))
        if shard_id == self.stop_on_shard:
            stop.set()
        if self.error is not None and shard_id == 0:
            raise self.error
        return False


def config(**overrides):
    values = {
        "shard_count": 1,
        "owner_renew_interval_seconds": 0.01,
        "callback_poll_seconds": 0.01,
        "shutdown_timeout_seconds": 0.1,
        "processing_timeout_seconds": 2,
        "redis": ExpiryRedisConfig.model_construct(request_budget_seconds=0.05),
    }
    values.update(overrides)
    return ExpiryConfig.model_construct(**values)


def consumer(
    store,
    handler,
    *,
    max_attempts=3,
    metadata_enabled=True,
    metrics=None,
    timer_config=None,
):
    timer_config = timer_config or config()
    metrics = metrics or ExpiryMetrics()
    coordinator = ShardLeaseCoordinator(store, timer_config, metrics)
    shard_runner = ShardRunner(coordinator, timer_config, metrics)
    binding = HandlerBinding(
        handler,
        HandlerOptions(
            concurrency=1,
            max_attempts=max_attempts,
            metadata_enabled=metadata_enabled,
        ),
    )
    return (
        ExpiryConsumer(
            store,
            shard_runner,
            "session",
            binding,
            timer_config,
            metrics,
            start_shard=0,
        ),
        shard_runner,
    )


def worker_consumer(store, shard_runner, timer_config):
    return ExpiryConsumer(
        store,
        shard_runner,
        "session",
        HandlerBinding(Handler(), HandlerOptions()),
        timer_config,
        ExpiryMetrics(),
        start_shard=0,
    )


@pytest.mark.asyncio
async def test_empty_registry_finishes_and_releases_owner_without_callback():
    store = Store(empty=True)
    handler = Handler()
    resource_consumer, shard_runner = consumer(store, handler)
    assert (
        await asyncio.wait_for(
            shard_runner.run_if_acquired(
                OwnerRole.CALLBACK,
                0,
                resource_consumer.process_shard,
                asyncio.Event(),
                item_type="session",
            ),
            timeout=2.0,
        )
        is True
    )
    assert handler.requests == []
    assert store.claim_requests == []
    assert len(store.released) == 1
    assert shard_runner.has_pending is False


@pytest.mark.asyncio
async def test_completed_callback_with_missing_metadata_acks_claim_and_releases_owner():
    store = Store()
    handler = Handler()
    handler.release.set()
    resource_consumer, shard_runner = consumer(store, handler)
    assert (
        await asyncio.wait_for(
            shard_runner.run_if_acquired(
                OwnerRole.CALLBACK,
                0,
                resource_consumer.process_shard,
                asyncio.Event(),
                item_type="session",
            ),
            timeout=2.0,
        )
        is True
    )
    assert [request.identity for request in handler.requests] == [store.identity]
    assert store.metadata_reads == [store.item_id]
    assert store.acked == [store.claims[0]]
    assert store.claim_requests[0][2]["attempt"] == 0
    assert len(store.released) == 1
    assert shard_runner.has_pending is False


@pytest.mark.asyncio
async def test_completed_callback_skips_metadata_lookup_when_disabled():
    store = Store()
    handler = Handler()
    handler.release.set()
    resource_consumer, shard_runner = consumer(store, handler, metadata_enabled=False)

    assert (
        await asyncio.wait_for(
            shard_runner.run_if_acquired(
                OwnerRole.CALLBACK,
                0,
                resource_consumer.process_shard,
                asyncio.Event(),
                item_type="session",
            ),
            timeout=2.0,
        )
        is True
    )
    assert [request.identity for request in handler.requests] == [store.identity]
    assert store.metadata_reads == []
    assert store.acked == [store.claims[0]]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["retry", "exception"])
@pytest.mark.parametrize("last_attempt", [False, True])
async def test_failed_callback_advances_immediately_and_acks_after_last_attempt(
    failure, last_attempt
):
    store = Store(attempt=1 if last_attempt else 0)
    handler = Handler(
        status=CallbackStatus.RETRY,
        error=RuntimeError("callback failed") if failure == "exception" else None,
    )
    resource_consumer, shard_runner = consumer(store, handler, max_attempts=2)
    stop = asyncio.Event()
    run_task = asyncio.create_task(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            stop,
            item_type="session",
        )
    )
    try:
        await asyncio.wait_for(handler.started.wait(), timeout=2.0)
        callback_tasks = set(resource_consumer._callback_tasks)
        assert callback_tasks
        handler.release.set()
        await asyncio.wait_for(handler.finished.wait(), timeout=2.0)
        finished, pending = await asyncio.wait(callback_tasks, timeout=2.0)
        assert finished == callback_tasks
        assert not pending
        assert await asyncio.wait_for(run_task, timeout=2.0) is True
        assert len(store.acked) == 1
        assert len(store.advanced) == (0 if last_attempt else 1)
        assert len(handler.requests) == (1 if last_attempt else 2)
        assert store.deferred == []
        assert len(store.released) == 1
        assert shard_runner.has_pending is False
    finally:
        stop.set()
        handler.release.set()
        await asyncio.wait_for(asyncio.gather(run_task, return_exceptions=True), timeout=2.0)
        assert await asyncio.wait_for(shard_runner.wait_pending(1.0), timeout=2.0) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("result", ["completed", "retry", "exception"])
@pytest.mark.parametrize("last_attempt", [False, True])
async def test_cancelled_callback_never_acks_after_drain_when_handler_suppresses_cancel(
    result, last_attempt
):
    store = Store(attempt=1 if last_attempt else 0)
    handler = Handler(
        status=CallbackStatus.COMPLETED if result == "completed" else CallbackStatus.RETRY,
        error=RuntimeError("callback failed") if result == "exception" else None,
        resist_cancellation=True,
    )
    resource_consumer, shard_runner = consumer(store, handler, max_attempts=2)
    stop = asyncio.Event()
    run_task = asyncio.create_task(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            stop,
            item_type="session",
        )
    )
    try:
        await asyncio.wait_for(handler.started.wait(), timeout=2.0)
        with pytest.raises(StoreOperationError) as error:
            await resource_consumer.drain(0)
        assert error.value.code == "shard_not_stopped"
        await asyncio.wait_for(handler.cancelled.wait(), timeout=2.0)
        # Keep the shard running: cancellation of the child alone must block ACK,
        # without relying on owner loss or the runner's shutdown revocation.
        assert not run_task.done()
        assert store.acked == []
        assert store.released == []
        handler.release.set()
        await asyncio.wait_for(handler.finished.wait(), timeout=2.0)
        await resource_consumer.drain(1.0)
        assert store.acked == []
        stop.set()
        assert await asyncio.wait_for(run_task, timeout=2.0) is True
        assert len(handler.requests) == 1
        assert store.released == []
        assert shard_runner.has_pending is False
    finally:
        stop.set()
        handler.release.set()
        await asyncio.wait_for(asyncio.gather(run_task, return_exceptions=True), timeout=2.0)
        assert await asyncio.wait_for(shard_runner.wait_pending(1.0), timeout=2.0) is True


@pytest.mark.asyncio
async def test_timed_out_last_attempt_is_retried_without_becoming_exhausted():
    store = Store(attempt=2)
    handler = Handler()
    handler.release.set()
    resource_consumer, shard_runner = consumer(store, handler)
    await asyncio.wait_for(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            asyncio.Event(),
            item_type="session",
        ),
        timeout=2.0,
    )
    assert len(store.acked) == 1
    assert len(store.claims) == 1
    assert decode_claim_score(store.claims[0].claim_score)[0] == 2
    assert len(handler.requests) == 1
    assert len(store.released) == 1
    assert shard_runner.has_pending is False


@pytest.mark.asyncio
async def test_callback_timeout_is_handled_as_a_failed_attempt():
    store = Store()
    handler = Handler()
    events = []
    metrics = ExpiryMetrics(lambda name, value, labels: events.append((name, value, labels)))
    resource_consumer, shard_runner = consumer(
        store,
        handler,
        max_attempts=1,
        metrics=metrics,
        timer_config=config(processing_timeout_seconds=0.01),
    )

    assert (
        await asyncio.wait_for(
            shard_runner.run_if_acquired(
                OwnerRole.CALLBACK,
                0,
                resource_consumer.process_shard,
                asyncio.Event(),
                item_type="session",
            ),
            timeout=2.0,
        )
        is True
    )

    assert handler.cancelled.is_set()
    assert store.acked == [store.claims[0]]
    assert any(
        name == "timer_callback_exhausted" and labels["outcome"] == "callback_timeout"
        for name, _, labels in events
    )


@pytest.mark.asyncio
async def test_waiting_bucket_defers_registration_to_exact_timeout_deadline():
    store = Store(now_ms=124_000)
    handler = Handler()
    resource_consumer, shard_runner = consumer(store, handler)

    await asyncio.wait_for(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            asyncio.Event(),
            item_type="session",
        ),
        timeout=2.0,
    )

    assert handler.requests == []
    assert store.claim_requests == []
    assert len(store.deferred) == 1
    assert store.deferred[0][2] == 125_000


@pytest.mark.asyncio
async def test_waiting_attempt_does_not_block_ready_item_in_next_attempt():
    store = Store(now_ms=124_000)
    retry_identity = ItemIdentity("session", "account", "user", "retry")
    retry_item_id = encode_item_id(retry_identity)
    retry_score = encode_ready_claim_score(1)

    async def read_bucket_page(bucket, *, attempt, limit):
        if attempt == 0:
            return (
                BucketPage(())
                if store.score is None
                else BucketPage((BucketItem(store.item_id, store.score),))
            )
        if attempt == 1 and retry_score is not None:
            return BucketPage((BucketItem(retry_item_id, retry_score),))
        return BucketPage(())

    async def claim(bucket, item, *, attempt, started_at_seconds):
        nonlocal retry_score
        store.claim_requests.append(
            (bucket, item, {"attempt": attempt, "started_at_seconds": started_at_seconds})
        )
        if item.item_id != retry_item_id or item.score != retry_score:
            return None
        retry_score = encode_claim_score(attempt, started_at_seconds)
        result = Claim(bucket, item.item_id, retry_score)
        store.claims.append(result)
        return result

    async def ack(claim):
        nonlocal retry_score
        if claim.item_id != retry_item_id or claim.claim_score != retry_score:
            return False
        retry_score = None
        store.acked.append(claim)
        return True

    store.read_bucket_page = read_bucket_page
    store.claim = claim
    store.ack = ack
    handler = Handler()
    handler.release.set()
    resource_consumer, shard_runner = consumer(store, handler)

    await asyncio.wait_for(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            asyncio.Event(),
            item_type="session",
        ),
        timeout=2.0,
    )

    assert [request.identity for request in handler.requests] == [retry_identity]
    assert len(store.acked) == 1
    assert store.deferred[0][2] == 125_000


@pytest.mark.asyncio
async def test_claim_conflict_rescans_without_duplicate_callback():
    store = Store()
    original_claim = store.claim
    conflicts = 1

    async def claim_with_conflict(*args, **kwargs):
        nonlocal conflicts
        if conflicts:
            conflicts -= 1
            store.claim_requests.append((args[0], args[1], kwargs))
            return None
        return await original_claim(*args, **kwargs)

    store.claim = claim_with_conflict
    handler = Handler()
    handler.release.set()
    resource_consumer, shard_runner = consumer(store, handler)

    await asyncio.wait_for(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            asyncio.Event(),
            item_type="session",
        ),
        timeout=2.0,
    )

    assert len(store.claim_requests) == 2
    assert len(handler.requests) == 1
    assert len(store.acked) == 1


@pytest.mark.asyncio
async def test_last_explicit_failure_emits_exhausted_metric_and_removes_item():
    events = []
    metrics = ExpiryMetrics(lambda name, value, labels: events.append((name, value, labels)))
    store = Store(attempt=1)
    handler = Handler(status=CallbackStatus.RETRY)
    handler.release.set()
    resource_consumer, shard_runner = consumer(
        store,
        handler,
        max_attempts=2,
        metrics=metrics,
    )

    await asyncio.wait_for(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            asyncio.Event(),
            item_type="session",
        ),
        timeout=2.0,
    )

    assert store.score is None
    assert len(store.acked) == 1
    assert (
        "timer_callback_exhausted",
        1.0,
        {"item_type": "session", "outcome": "callback_retry"},
    ) in events


@pytest.mark.asyncio
async def test_shard_visit_filters_type_and_processes_only_one_minute():
    store = Store()
    second = BucketRef("session", 0, 120_000)
    watch = BucketRef("watch", 0, 60_000)

    async def read_registry_page(shard_id, *, after, limit):
        return RegistryPage((watch, store.bucket, second), None)

    store.read_registry_page = read_registry_page
    handler = Handler()
    handler.release.set()
    resource_consumer, shard_runner = consumer(store, handler)

    await shard_runner.run_if_acquired(
        OwnerRole.CALLBACK,
        0,
        resource_consumer.process_shard,
        asyncio.Event(),
        item_type="session",
    )

    assert len(handler.requests) == 1
    assert store.claim_requests[0][0] == store.bucket
    assert all(bucket != second for bucket, _, _ in store.claim_requests)


@pytest.mark.asyncio
async def test_owner_loss_keeps_resistant_callback_pending_and_prevents_late_ack():
    store = Store(lose_on_renew=True)
    handler = Handler(resist_cancellation=True)
    resource_consumer, shard_runner = consumer(store, handler)
    stop = asyncio.Event()
    run_task = asyncio.create_task(
        shard_runner.run_if_acquired(
            OwnerRole.CALLBACK,
            0,
            resource_consumer.process_shard,
            stop,
            item_type="session",
        )
    )
    next_run = None
    try:
        await asyncio.wait_for(handler.started.wait(), timeout=2.0)
        await asyncio.wait_for(store.renew_started.wait(), timeout=2.0)
        store.allow_renew.set()
        await asyncio.wait_for(handler.cancelled.wait(), timeout=2.0)
        finished, _ = await asyncio.wait({run_task}, timeout=0.3)
        assert run_task in finished, (
            "Consumer cleanup must not join a resistant callback indefinitely"
        )
        assert run_task.result() is True
        assert shard_runner.has_pending is True
        assert await asyncio.wait_for(shard_runner.wait_pending(0.01), timeout=2.0) is False
        assert store.acked == []
        assert store.released == []
        assert len(resource_consumer._callback_tasks) == 1

        store.lose_on_renew = False
        store.now_ms += 3_000
        next_run = asyncio.create_task(
            shard_runner.run_if_acquired(
                OwnerRole.CALLBACK,
                0,
                resource_consumer.process_shard,
                asyncio.Event(),
                item_type="session",
            )
        )

        async def wait_for_second_callback():
            while len(handler.requests) < 2:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_second_callback(), timeout=2.0)
        assert len(resource_consumer._callback_tasks) == 2

        handler.release.set()
        assert await asyncio.wait_for(next_run, timeout=2.0) is True
        assert await asyncio.wait_for(shard_runner.wait_pending(1.0), timeout=2.0) is True
        assert handler.finished.is_set()
        assert store.acked == [store.claims[1]]
        assert len(store.released) == 1
        assert not resource_consumer._callback_tasks
        assert shard_runner.has_pending is False
    finally:
        stop.set()
        store.allow_renew.set()
        handler.release.set()
        tasks = [run_task] if next_run is None else [run_task, next_run]
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2.0)
        assert await asyncio.wait_for(shard_runner.wait_pending(1.0), timeout=2.0) is True


@pytest.mark.asyncio
async def test_consumer_worker_delegates_callback_role_and_stops_mid_shard_round():
    store = Store(empty=True)
    shard_runner = Runner(stop_on_shard=1)
    resource_consumer = worker_consumer(store, shard_runner, config(shard_count=3))
    await asyncio.wait_for(resource_consumer.run(asyncio.Event()), timeout=2.0)
    assert [(role, item_type, shard_id) for role, item_type, shard_id, _ in shard_runner.calls] == [
        (OwnerRole.CALLBACK, "session", 0),
        (OwnerRole.CALLBACK, "session", 1),
    ]
    assert all(
        process == resource_consumer.process_shard for _, _, _, process in shard_runner.calls
    )


@pytest.mark.asyncio
async def test_consumer_worker_does_not_enter_runner_when_already_stopped():
    shard_runner = Runner()
    resource_consumer = worker_consumer(Store(empty=True), shard_runner, config())
    stop = asyncio.Event()
    stop.set()
    await resource_consumer.run(stop)
    assert shard_runner.calls == []


@pytest.mark.asyncio
async def test_consumer_worker_continues_after_poll_timeout():
    stop = asyncio.Event()

    class PollingRunner:
        def __init__(self):
            self.calls = 0

        async def run_if_acquired(self, role, shard_id, process, observed_stop, *, item_type=None):
            self.calls += 1
            if self.calls == 2:
                observed_stop.set()
            return False

    runner = PollingRunner()
    resource_consumer = worker_consumer(
        Store(empty=True),
        runner,
        config(callback_poll_seconds=0.001),
    )

    await asyncio.wait_for(resource_consumer.run(stop), timeout=2.0)

    assert runner.calls == 2


@pytest.mark.asyncio
async def test_consumer_worker_warns_and_continues_after_unfinished_shard(monkeypatch):
    shard_runner = Runner(
        stop_on_shard=1,
        error=StoreOperationError("shard_not_stopped", "callback still running"),
    )
    resource_consumer = worker_consumer(Store(empty=True), shard_runner, config(shard_count=3))
    events = []
    resource_consumer._metrics = ExpiryMetrics(
        lambda name, value, labels: events.append((name, value, labels))
    )
    warnings = []
    monkeypatch.setattr(
        consumer_module.logger,
        "warning",
        lambda *args, **kwargs: warnings.append((args, kwargs)),
    )

    await asyncio.wait_for(resource_consumer.run(asyncio.Event()), timeout=2.0)

    assert [shard_id for _, _, shard_id, _ in shard_runner.calls] == [0, 1]
    assert any(
        name == "timer_shard_round" and labels["outcome"] == "orphaned"
        for name, _, labels in events
    )
    assert warnings


@pytest.mark.asyncio
async def test_consumer_worker_continues_shard_round_after_processor_error():
    shard_runner = Runner(stop_on_shard=1, error=RuntimeError("callback failed"))
    resource_consumer = worker_consumer(Store(empty=True), shard_runner, config(shard_count=3))
    await asyncio.wait_for(resource_consumer.run(asyncio.Event()), timeout=2.0)
    assert [shard_id for _, _, shard_id, _ in shard_runner.calls] == [0, 1]


@pytest.mark.asyncio
async def test_consumer_worker_propagates_cancellation_instead_of_retrying():
    shard_runner = Runner(error=asyncio.CancelledError())
    resource_consumer = worker_consumer(Store(empty=True), shard_runner, config())
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(resource_consumer.run(asyncio.Event()), timeout=2.0)
    assert len(shard_runner.calls) == 1
