import asyncio

import pytest

from openviking.scheduling.expiry import ItemIdentity
from openviking.scheduling.expiry.config import ExpiryConfig
from openviking.scheduling.expiry.keys import encode_item_id
from openviking.scheduling.expiry.metrics import ExpiryMetrics
from openviking.scheduling.expiry.models import (
    OwnerRole,
    OwnerToken,
    PublishResult,
    ShardLease,
    ShardWindow,
    SourceCursor,
    SourceItem,
    SourcePage,
    StoreOperationError,
)
from openviking.scheduling.expiry.publisher import ExpiryPublisher, _wait_until_next_round
from openviking.scheduling.expiry.shard_runner import ShardExecution


class Store:
    def __init__(self, *, item=None, items=None, cleanup_results=None, upper_ms=60_000):
        self.items = list(items if items is not None else (() if item is None else (item,)))
        self.progress = 0
        self.upper_ms = upper_ms
        self.staged = []
        self.published = []
        self.cleaned = []
        self.cleanup_results = list(cleanup_results or ())
        self.resets = []

    async def read_scan_window(self, shard_id):
        return ShardWindow(self.progress, self.upper_ms, self.upper_ms + 123)

    async def cleanup_source_page(self, shard_id, through_ms, *, limit):
        self.cleaned.append((through_ms, limit))
        return self.cleanup_results.pop(0) if self.cleanup_results else 0

    async def read_source_page(
        self, shard_id, *, lower_exclusive_ms, upper_inclusive_ms, after, limit
    ):
        items = sorted(self.items, key=lambda item: (item.due_at_ms, item.item_id))
        items = [
            item
            for item in items
            if lower_exclusive_ms < item.due_at_ms <= upper_inclusive_ms
            and (after is None or (item.due_at_ms, item.item_id) > (after.due_at_ms, after.item_id))
        ]
        page_items = items[:limit]
        next_cursor = None
        if len(items) > limit:
            last = page_items[-1]
            next_cursor = SourceCursor(last.due_at_ms, last.item_id)
        return SourcePage(tuple(page_items), next_cursor)

    async def reset_unpublished_minute(self, owner, **kwargs):
        self.resets.append(kwargs)
        return None

    async def stage_items(self, owner, bucket, item_ids):
        self.staged.append((bucket, tuple(item_ids)))

    async def publish_minute(self, owner, **kwargs):
        self.published.append(kwargs)
        self.progress = kwargs["minute_ms"]
        return PublishResult("published", kwargs["minute_ms"])


def lease():
    return ShardLease(
        OwnerToken(OwnerRole.SCHEDULER, 0, "token", 9999999999999),
        asyncio.Event(),
        float("inf"),
    )


def config():
    return ExpiryConfig.model_construct(
        shard_count=1,
        scan_batch_size=8,
        cleanup_batch_size=8,
        registry_page_size=8,
    )


@pytest.mark.asyncio
async def test_round_wait_timeout_is_a_normal_scheduler_tick():
    await _wait_until_next_round(asyncio.Event(), 0.001)


@pytest.mark.asyncio
async def test_empty_scan_does_not_publish_or_advance_progress():
    store = Store()
    scheduler = ExpiryPublisher(store, None, config(), ExpiryMetrics())
    await scheduler.process_shard(ShardExecution(lease()), asyncio.Event())
    assert store.published == []
    assert store.cleaned == [(0, 8)]


@pytest.mark.asyncio
async def test_non_empty_minute_is_fully_staged_before_publish():
    item_id = encode_item_id(ItemIdentity("session", "a", "u", "s"))
    store = Store(item=SourceItem(item_id, 60_000))
    scheduler = ExpiryPublisher(store, None, config(), ExpiryMetrics())
    await scheduler.process_shard(ShardExecution(lease()), asyncio.Event())
    assert store.staged[0][1] == (item_id,)
    assert store.published[0]["minute_ms"] == 60_000
    assert store.cleaned == [(0, 8), (60_000, 8), (60_000, 8)]
    assert store.resets[0]["item_types"] == ("session",)


@pytest.mark.asyncio
async def test_source_cleanup_drains_bounded_pages():
    store = Store(cleanup_results=[8, 8, 3])
    scheduler = ExpiryPublisher(store, None, config(), ExpiryMetrics())

    await scheduler.process_shard(ShardExecution(lease()), asyncio.Event())

    assert store.cleaned == [(0, 8), (0, 8), (0, 8)]


@pytest.mark.asyncio
async def test_source_cleanup_stops_between_pages():
    stop = asyncio.Event()

    class StoppingStore(Store):
        async def cleanup_source_page(self, shard_id, through_ms, *, limit):
            removed = await super().cleanup_source_page(shard_id, through_ms, limit=limit)
            stop.set()
            return removed

    store = StoppingStore(cleanup_results=[8, 8])
    scheduler = ExpiryPublisher(store, None, config(), ExpiryMetrics())

    await scheduler.process_shard(ShardExecution(lease()), stop)

    assert store.cleaned == [(0, 8)]


@pytest.mark.asyncio
async def test_source_cleanup_rechecks_lease_between_pages():
    shard_lease = lease()

    class LosingStore(Store):
        async def cleanup_source_page(self, shard_id, through_ms, *, limit):
            removed = await super().cleanup_source_page(shard_id, through_ms, limit=limit)
            shard_lease.lost.set()
            return removed

    store = LosingStore(cleanup_results=[8, 8])
    scheduler = ExpiryPublisher(store, None, config(), ExpiryMetrics())

    with pytest.raises(StoreOperationError, match="Shard lease is no longer valid"):
        await scheduler.process_shard(ShardExecution(shard_lease), asyncio.Event())

    assert store.cleaned == [(0, 8)]


@pytest.mark.asyncio
async def test_unsupported_source_item_is_cleaned_without_advancing_progress(timer_logs):
    item_id = encode_item_id(ItemIdentity("watch", "a", "u", "s"))
    store = Store(item=SourceItem(item_id, 60_000))
    events = []
    metrics = ExpiryMetrics(lambda name, value, labels: events.append((name, value, labels)))
    scheduler = ExpiryPublisher(store, None, config(), metrics)

    await scheduler.process_shard(ShardExecution(lease()), asyncio.Event())

    assert store.staged == []
    assert store.published == []
    assert store.progress == 0
    assert store.cleaned == [(0, 8), (60_000, 8)]
    assert events == [
        (
            "timer_source_invalid",
            1.0,
            {"item_type": "", "outcome": "unsupported_type"},
        )
    ]
    assert "reason=unsupported_type count=1" in timer_logs.text
    assert item_id not in timer_logs.text


@pytest.mark.asyncio
async def test_all_invalid_minute_does_not_block_later_valid_minute():
    invalid_id = encode_item_id(ItemIdentity("watch", "a", "u", "invalid"))
    valid_id = encode_item_id(ItemIdentity("session", "a", "u", "valid"))
    store = Store(
        items=(
            SourceItem(invalid_id, 60_000),
            SourceItem(valid_id, 120_000),
        ),
        upper_ms=120_000,
    )
    scheduler = ExpiryPublisher(store, None, config(), ExpiryMetrics())

    await scheduler.process_shard(ShardExecution(lease()), asyncio.Event())

    assert store.staged[0][1] == (valid_id,)
    assert store.published[0]["expected_progress_ms"] == 0
    assert store.published[0]["minute_ms"] == 120_000
    assert store.progress == 120_000
    assert store.cleaned == [(0, 8), (60_000, 8), (120_000, 8), (120_000, 8)]


@pytest.mark.asyncio
async def test_malformed_source_item_is_skipped_while_valid_item_is_published(
    timer_logs,
):
    valid_id = encode_item_id(ItemIdentity("session", "a", "u", "valid"))
    malformed_id = "not-an-item-id"
    store = Store(
        items=(
            SourceItem(valid_id, 60_000),
            SourceItem(malformed_id, 60_000),
        )
    )
    scheduler = ExpiryPublisher(store, None, config(), ExpiryMetrics())

    await scheduler.process_shard(ShardExecution(lease()), asyncio.Event())

    assert store.staged[0][1] == (valid_id,)
    assert len(store.published[0]["buckets"]) == 1
    assert store.published[0]["minute_ms"] == 60_000
    assert store.cleaned == [(0, 8), (60_000, 8), (60_000, 8)]
    assert "reason=invalid_identity count=1" in timer_logs.text
    assert malformed_id not in timer_logs.text


@pytest.fixture
def timer_logs(caplog):
    from openviking.scheduling.expiry.metrics import logger

    logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


class Runner:
    def __init__(self, *, stop_on_shard=0, error=None):
        self.stop_on_shard = stop_on_shard
        self.error = error
        self.calls = []

    async def run_if_acquired(self, role, shard_id, process, stop):
        self.calls.append((role, shard_id, process))
        if shard_id == self.stop_on_shard:
            stop.set()
        if self.error is not None and shard_id == 0:
            raise self.error
        return False


@pytest.mark.asyncio
async def test_scheduler_worker_delegates_scheduler_role_and_stops_mid_shard_round():
    shard_runner = Runner(stop_on_shard=1)
    timer_config = ExpiryConfig.model_construct(shard_count=3)
    scheduler = ExpiryPublisher(Store(), shard_runner, timer_config, ExpiryMetrics())
    await asyncio.wait_for(scheduler.run(asyncio.Event()), timeout=2.0)
    assert [(role, shard_id) for role, shard_id, _ in shard_runner.calls] == [
        (OwnerRole.SCHEDULER, 0),
        (OwnerRole.SCHEDULER, 1),
    ]
    assert all(process == scheduler.process_shard for _, _, process in shard_runner.calls)


@pytest.mark.asyncio
async def test_scheduler_worker_does_not_enter_runner_when_already_stopped():
    shard_runner = Runner()
    scheduler = ExpiryPublisher(Store(), shard_runner, config(), ExpiryMetrics())
    stop = asyncio.Event()
    stop.set()
    await scheduler.run(stop)
    assert shard_runner.calls == []


@pytest.mark.asyncio
async def test_scheduler_worker_propagates_cancellation_instead_of_retrying():
    shard_runner = Runner(error=asyncio.CancelledError())
    scheduler = ExpiryPublisher(Store(), shard_runner, config(), ExpiryMetrics())
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(scheduler.run(asyncio.Event()), timeout=2.0)
    assert len(shard_runner.calls) == 1


@pytest.mark.asyncio
async def test_scheduler_worker_propagates_pending_work_instead_of_acquiring_next_shard():
    shard_runner = Runner(
        stop_on_shard=1,
        error=StoreOperationError("shard_not_stopped", "scheduler still running"),
    )
    timer_config = ExpiryConfig.model_construct(shard_count=3)
    scheduler = ExpiryPublisher(Store(), shard_runner, timer_config, ExpiryMetrics())
    with pytest.raises(StoreOperationError) as error:
        await asyncio.wait_for(scheduler.run(asyncio.Event()), timeout=2.0)
    assert error.value.code == "shard_not_stopped"
    assert len(shard_runner.calls) == 1


@pytest.mark.asyncio
async def test_scheduler_worker_continues_shard_round_after_processor_error():
    shard_runner = Runner(stop_on_shard=1, error=RuntimeError("scan failed"))
    timer_config = ExpiryConfig.model_construct(shard_count=3)
    scheduler = ExpiryPublisher(Store(), shard_runner, timer_config, ExpiryMetrics())
    await asyncio.wait_for(scheduler.run(asyncio.Event()), timeout=2.0)
    assert [shard_id for _, shard_id, _ in shard_runner.calls] == [0, 1]
