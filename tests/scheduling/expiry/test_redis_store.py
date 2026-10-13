from unittest.mock import AsyncMock

import pytest

from openviking.scheduling.expiry.config import ExpiryRedisConfig
from openviking.scheduling.expiry.keys import ExpiryKeys
from openviking.scheduling.expiry.metrics import ExpiryMetrics
from openviking.scheduling.expiry.models import (
    BucketItem,
    BucketRef,
    Claim,
    OwnerRole,
    OwnerToken,
    RegistryCursor,
    SourceCursor,
    SourceItem,
    StoreOperationError,
    encode_claim_score,
    encode_ready_claim_score,
)
from openviking.scheduling.expiry.redis_store import RedisExpiryStore


class InlineExecutor:
    async def run(self, operation):
        return await operation()


class FlakyClient:
    def __init__(self, failures):
        self.failures = list(failures)
        self.calls = 0

    async def get(self, _key):
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return "value"


class ScriptClient:
    def __init__(self):
        self.calls = []

    async def evalsha(self, sha, key_count, *args):
        self.calls.append(("evalsha", sha, key_count, args))
        error_type = type("NoScriptError", (RuntimeError,), {})
        raise error_type("No matching script. Please use EVAL.")

    async def eval(self, source, key_count, *args):
        self.calls.append(("eval", source, key_count, args))
        return "loaded"


class ClusterTimeClient:
    def __init__(self):
        self.node = object()
        self.key = None
        self.target = None

    def get_node_from_key(self, key):
        self.key = key
        return self.node

    async def time(self, *, target_nodes):
        self.target = target_nodes
        return (123, 456_789)


def store(client):
    result = RedisExpiryStore(
        ExpiryRedisConfig(), ExpiryKeys("resource"), InlineExecutor(), ExpiryMetrics()
    )
    result._client = client
    return result


@pytest.mark.asyncio
async def test_safe_command_retries_transient_redis_failure():
    client = FlakyClient([ConnectionError("temporary")])

    assert await store(client)._command("get", "key", retry="read") == "value"
    assert client.calls == 2


@pytest.mark.asyncio
async def test_uncertain_write_is_left_for_caller_to_resolve():
    client = FlakyClient([ConnectionError("temporary")])

    with pytest.raises(StoreOperationError) as raised:
        await store(client)._command("get", "key", retry="resolve_by_caller")

    assert client.calls == 1
    assert raised.value.outcome_unknown is True


@pytest.mark.asyncio
async def test_protocol_error_is_not_retried():
    client = FlakyClient([RuntimeError("wrong_key_type")])

    with pytest.raises(StoreOperationError) as raised:
        await store(client)._command("get", "key", retry="idempotent")

    assert client.calls == 1
    assert raised.value.code == "wrong_key_type"


@pytest.mark.asyncio
async def test_evalsha_falls_back_to_eval_on_cache_miss():
    client = ScriptClient()
    timer_store = store(client)

    result = await timer_store._eval("upsert_resource", ["source"], ["item", 1], retry="read")

    assert result == "loaded"
    assert [call[0] for call in client.calls] == ["evalsha", "eval"]


@pytest.mark.asyncio
async def test_bucket_reset_uses_single_key_cluster_commands(monkeypatch):
    timer_store = store(FlakyClient([]))
    monkeypatch.setattr(timer_store, "assert_owner", AsyncMock())
    monkeypatch.setattr(timer_store, "read_progress", AsyncMock(return_value=0))
    command = AsyncMock()
    monkeypatch.setattr(timer_store, "_command", command)

    await timer_store.reset_unpublished_minute(
        OwnerToken(OwnerRole.SCHEDULER, 3, "owner", 999),
        minute_ms=60_000,
        expected_progress_ms=0,
        item_types=("session", "watch"),
    )

    assert [call.args[:2] for call in command.await_args_list] == [
        ("unlink", timer_store._keys.bucket(BucketRef("session", 3, 60_000))),
        ("unlink", timer_store._keys.bucket(BucketRef("watch", 3, 60_000))),
    ]


@pytest.mark.asyncio
async def test_source_cleanup_page_uses_single_key_atomic_script(monkeypatch):
    timer_store = store(FlakyClient([]))
    evaluate = AsyncMock(return_value=7)
    monkeypatch.setattr(timer_store, "_eval", evaluate)

    removed = await timer_store.cleanup_source_page(3, 60_000, limit=128)

    assert removed == 7
    evaluate.assert_awaited_once_with(
        "cleanup_source_page",
        [timer_store._keys.source(3)],
        [60_000, 128],
        retry="idempotent",
    )


@pytest.mark.asyncio
async def test_registry_page_uses_score_cursor(monkeypatch):
    timer_store = store(FlakyClient([]))
    evaluate = AsyncMock(return_value='{"items":[["session:3:60000","70000"]],"next":null}')
    monkeypatch.setattr(timer_store, "_eval", evaluate)

    page = await timer_store.read_registry_page(
        3,
        after=RegistryCursor(65_000, "session:3:0"),
        limit=128,
    )

    assert page.buckets == (BucketRef("session", 3, 60_000),)
    evaluate.assert_awaited_once_with(
        "read_zset_page",
        [timer_store._keys.registry(3)],
        [-1, "", 65_000, "session:3:0", 128],
        retry="read",
    )


@pytest.mark.asyncio
async def test_waiting_bucket_can_defer_shared_registration(monkeypatch):
    timer_store = store(FlakyClient([]))
    evaluate = AsyncMock(return_value=1)
    monkeypatch.setattr(timer_store, "_eval", evaluate)
    bucket = BucketRef("session", 3, 60_000)
    owner = OwnerToken(
        OwnerRole.CALLBACK,
        3,
        "owner",
        999,
        "session",
    )

    assert (
        await timer_store.defer_registration(
            owner,
            bucket,
            next_poll_at_ms=180_000,
        )
        is True
    )

    evaluate.assert_awaited_once_with(
        "defer_registration",
        [
            timer_store._keys.registry(3),
            timer_store._keys.owner(
                OwnerRole.CALLBACK,
                3,
                item_type="session",
            ),
        ],
        ["owner", timer_store._keys.registry_member(bucket), 180_000],
        retry="idempotent",
    )


@pytest.mark.asyncio
async def test_redis_time_is_returned_in_milliseconds(monkeypatch):
    timer_store = store(FlakyClient([]))
    command = AsyncMock(return_value=(123, 456_789))
    monkeypatch.setattr(timer_store, "_command", command)

    assert await timer_store.read_time_ms() == 123_456
    command.assert_awaited_once_with("time")


@pytest.mark.asyncio
async def test_cluster_time_is_routed_to_key_primary():
    client = ClusterTimeClient()
    timer_store = store(client)
    timer_store._config = ExpiryRedisConfig.model_construct(mode="cluster")

    assert await timer_store._read_time_ms_for_key("source-key") == 123_456
    assert client.key == "source-key"
    assert client.target is client.node


@pytest.mark.asyncio
async def test_source_first_page_uses_native_score_range(monkeypatch):
    timer_store = store(FlakyClient([]))
    command = AsyncMock(return_value=[("a", 100.0), ("b", 100.0), ("c", 200.0)])
    monkeypatch.setattr(timer_store, "_command", command)

    page = await timer_store.read_source_page(
        3,
        lower_exclusive_ms=0,
        upper_inclusive_ms=1_000,
        after=None,
        limit=2,
    )

    assert page.items == (SourceItem("a", 100), SourceItem("b", 100))
    assert page.next_cursor == SourceCursor(100, "b")
    command.assert_awaited_once_with(
        "zrangebyscore",
        timer_store._keys.source(3),
        "(0",
        1_000,
        start=0,
        num=3,
        withscores=True,
    )


@pytest.mark.asyncio
async def test_source_continuation_uses_stable_cursor_script(monkeypatch):
    timer_store = store(FlakyClient([]))
    evaluate = AsyncMock(return_value='{"items":[["c","200"]],"next":["200","c"]}')
    monkeypatch.setattr(timer_store, "_eval", evaluate)

    page = await timer_store.read_source_page(
        3,
        lower_exclusive_ms=0,
        upper_inclusive_ms=1_000,
        after=SourceCursor(100, "b"),
        limit=2,
    )

    assert page.items == (SourceItem("c", 200),)
    assert page.next_cursor == SourceCursor(200, "c")
    evaluate.assert_awaited_once_with(
        "read_zset_page",
        [timer_store._keys.source(3)],
        [0, 1_000, 100, "b", 2],
        retry="read",
    )


@pytest.mark.asyncio
async def test_bucket_page_reads_one_attempt_layer_from_offset_zero(monkeypatch):
    timer_store = store(FlakyClient([]))
    command = AsyncMock(return_value=[("item", float(encode_claim_score(2, 123)))])
    monkeypatch.setattr(timer_store, "_command", command)
    bucket = BucketRef("session", 3, 60_000)

    page = await timer_store.read_bucket_page(
        bucket,
        attempt=2,
        limit=128,
    )

    assert page.items == (BucketItem("item", encode_claim_score(2, 123)),)
    lower = encode_claim_score(2, 1)
    upper = encode_claim_score(3, 1) - 2
    command.assert_awaited_once_with(
        "zrangebyscore",
        timer_store._keys.bucket(bucket),
        lower,
        upper,
        start=0,
        num=128,
        withscores=True,
    )


@pytest.mark.asyncio
async def test_claim_and_retry_use_score_compare_and_set(monkeypatch):
    timer_store = store(FlakyClient([]))
    evaluate = AsyncMock(side_effect=[1, 1])
    monkeypatch.setattr(timer_store, "_eval", evaluate)
    bucket = BucketRef("session", 3, 60_000)
    old_score = encode_claim_score(1, 123)

    claim = await timer_store.claim(
        bucket,
        BucketItem("item", old_score),
        attempt=1,
        started_at_seconds=200,
    )

    assert claim == Claim(bucket, "item", encode_claim_score(1, 200))
    evaluate.assert_awaited_once_with(
        "cas_zset_member",
        [timer_store._keys.bucket(bucket)],
        ["set", "item", old_score, encode_claim_score(1, 200)],
        retry="resolve_by_caller",
    )

    evaluate.reset_mock()
    assert await timer_store.advance_retry(claim) is True
    evaluate.assert_awaited_once_with(
        "cas_zset_member",
        [timer_store._keys.bucket(bucket)],
        ["set", "item", claim.claim_score, encode_ready_claim_score(2)],
        retry="resolve_by_caller",
    )


@pytest.mark.asyncio
async def test_ack_uses_score_compare_and_delete(monkeypatch):
    timer_store = store(FlakyClient([]))
    evaluate = AsyncMock(return_value=1)
    monkeypatch.setattr(timer_store, "_eval", evaluate)
    claim = Claim(BucketRef("session", 3, 60_000), "item", encode_claim_score(1, 123))

    assert await timer_store.ack(claim) is True
    evaluate.assert_awaited_once_with(
        "cas_zset_member",
        [timer_store._keys.bucket(claim.bucket)],
        ["delete", "item", claim.claim_score, ""],
        retry="idempotent",
    )


@pytest.mark.asyncio
async def test_owner_acquire_uses_native_set_nx_px(monkeypatch):
    timer_store = store(FlakyClient([]))
    command = AsyncMock(return_value=True)
    monkeypatch.setattr(timer_store, "_command", command)

    owner = await timer_store.try_acquire_owner(
        OwnerRole.CALLBACK,
        3,
        "token",
        120,
        item_type="session",
    )

    assert owner is not None
    assert owner.token == "token"
    command.assert_awaited_once_with(
        "set",
        timer_store._keys.owner(OwnerRole.CALLBACK, 3, item_type="session"),
        "token",
        nx=True,
        px=120_000,
        retry="resolve_by_caller",
    )


@pytest.mark.asyncio
async def test_owner_renew_and_release_share_compare_script(monkeypatch):
    timer_store = store(FlakyClient([]))
    evaluate = AsyncMock(side_effect=[1, 1])
    monkeypatch.setattr(timer_store, "_eval", evaluate)
    owner = OwnerToken(OwnerRole.SCHEDULER, 3, "token", 1)
    key = timer_store._keys.owner(OwnerRole.SCHEDULER, 3)

    assert await timer_store.renew_owner(owner, 120) is not None
    assert await timer_store.release_owner(owner) is True
    assert evaluate.await_args_list[0].args == (
        "compare_owner",
        [key],
        ["renew", "token", 120_000],
    )
    assert evaluate.await_args_list[0].kwargs == {"retry": "resolve_by_caller"}
    assert evaluate.await_args_list[1].args == (
        "compare_owner",
        [key],
        ["release", "token", ""],
    )
    assert evaluate.await_args_list[1].kwargs == {"retry": "idempotent"}
