import asyncio
from unittest.mock import AsyncMock, Mock

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


@pytest.fixture
async def cluster_store(monkeypatch):
    cluster = pytest.importorskip("redis.asyncio.cluster")
    timer_store = store(None)
    timer_store._config = ExpiryRedisConfig(mode="cluster", endpoints=["127.0.0.1:6379"])
    ping = AsyncMock(return_value=True)
    monkeypatch.setattr(cluster.RedisCluster, "ping", ping)
    await timer_store.open()
    ping.assert_awaited_once()
    try:
        yield timer_store
    finally:
        await timer_store.close()


def configure_cluster_node(monkeypatch, client, key, execute):
    cluster = pytest.importorskip("redis.asyncio.cluster")
    node = cluster.ClusterNode("127.0.0.1", 6379, server_type="primary")
    manager = client.nodes_manager
    manager.default_node = node
    manager.nodes_cache = {node.name: node}
    manager.slots_cache = {client.keyslot(key): [node]}

    async def initialize(_client):
        # Replace topology discovery only; execute_command and its retry/routing
        # loops remain the real redis-py implementation, including after close.
        _client._initialize = False
        return _client

    monkeypatch.setattr(cluster.RedisCluster, "initialize", initialize)
    parser = Mock(spec=type(client.commands_parser))
    parser.get_keys = AsyncMock(return_value=[key])
    client.commands_parser = parser
    monkeypatch.setattr(cluster.ClusterNode, "execute_command", execute)
    return node


@pytest.mark.asyncio
async def test_cluster_retry_settings_are_local_to_expiry_client(cluster_store):
    from redis.asyncio import Redis
    from redis.asyncio.cluster import RedisCluster

    client = cluster_store._client
    other_cluster = RedisCluster(host="127.0.0.1", port=6379)
    other_standalone = Redis(host="127.0.0.1", port=6379)
    try:
        assert client.cluster_error_retry_attempts == 0
        assert client.connection_error_retry_attempts == 0
        assert client.get_retry() is None
        assert all(
            node.connection_kwargs["retry"] is None
            for node in client.nodes_manager.startup_nodes.values()
        )
        assert other_cluster.cluster_error_retry_attempts == 3
        assert other_cluster.connection_error_retry_attempts == 3
        assert other_cluster.get_retry() is not None
        assert other_standalone.connection_pool.connection_kwargs["retry"] is None
    finally:
        await other_cluster.aclose()
        await other_standalone.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["DEL", "UNLINK"])
async def test_cluster_single_key_delete_bypasses_pipeline(
    monkeypatch, cluster_store, command
):
    key = f"single-key-{command.lower()}"
    calls = []

    async def execute(node, *args, **kwargs):
        calls.append((node.name, args, kwargs))
        return 1

    node = configure_cluster_node(monkeypatch, cluster_store._client, key, execute)

    result = await cluster_store._single_key_command(command, key, retry="idempotent")

    assert result == 1
    assert calls == [(node.name, (command, key), {})]


@pytest.mark.asyncio
async def test_standalone_client_constructor_is_unchanged(monkeypatch):
    redis_async = pytest.importorskip("redis.asyncio")
    client = Mock(spec=redis_async.Redis)
    client.ping = AsyncMock(return_value=True)
    client.aclose = AsyncMock()
    factory = Mock(return_value=client)
    monkeypatch.setattr(redis_async.Redis, "from_url", factory)
    timer_store = store(None)
    timer_store._config = ExpiryRedisConfig(endpoints=["127.0.0.1:6379"])
    try:
        await timer_store.open()
        factory.assert_called_once_with(
            "redis://127.0.0.1:6379",
            username=None,
            password=None,
            decode_responses=True,
            socket_connect_timeout=1.0,
            socket_timeout=2.0,
            max_connections=64,
        )
        client.ping.assert_awaited_once()
    finally:
        await timer_store.close()
    client.aclose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["claim", "owner"])
@pytest.mark.parametrize("error_name", ["ConnectionError", "TimeoutError"])
async def test_cluster_uncertain_write_is_not_replayed(
    monkeypatch, cluster_store, operation, error_name
):
    from redis import exceptions

    bucket = BucketRef("session", 3, 60_000)
    key = (
        cluster_store._keys.bucket(bucket)
        if operation == "claim"
        else cluster_store._keys.owner(OwnerRole.CALLBACK, 3, item_type="session")
    )
    expected_score = encode_claim_score(1, 123)
    state = {"score": expected_score, "owner": None, "calls": 0}

    async def execute(_node, *args, **kwargs):
        state["calls"] += 1
        if operation == "claim":
            assert args[0] == "EVALSHA"
            if state["score"] != int(args[-2]):
                return 0
            state["score"] = int(args[-1])
        else:
            assert args[0] == "SET"
            if state["owner"] is not None:
                return False
            state["owner"] = args[2]
        # The write takes effect before the response is lost. A second call
        # would return 0/False and hide the successful first execution.
        raise getattr(exceptions, error_name)("response lost after write")

    configure_cluster_node(monkeypatch, cluster_store._client, key, execute)
    with pytest.raises(StoreOperationError, match="response lost") as raised:
        if operation == "claim":
            await cluster_store.claim(
                bucket,
                BucketItem("item", expected_score),
                attempt=1,
                started_at_seconds=200,
            )
        else:
            await cluster_store.try_acquire_owner(
                OwnerRole.CALLBACK, 3, "token", 120, item_type="session"
            )
    assert raised.value.outcome_unknown is True
    assert state["calls"] == 1
    if operation == "claim":
        assert state["score"] == encode_claim_score(1, 200)
    else:
        assert state["owner"] == "token"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "idempotent"])
async def test_cluster_safe_operations_still_retry_in_store(monkeypatch, cluster_store, operation):
    from redis.exceptions import ConnectionError

    calls = []
    bucket = BucketRef("session", 3, 60_000)
    key = cluster_store._keys.bucket(bucket)

    async def execute(_node, *args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise ConnectionError("temporary")
        return "value" if operation == "read" else 1

    configure_cluster_node(monkeypatch, cluster_store._client, key, execute)
    if operation == "read":
        assert await cluster_store._command("get", key, retry="read") == "value"
    else:
        claim = Claim(bucket, "item", encode_claim_score(1, 123))
        assert await cluster_store.ack(claim) is True
    assert len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("redirect", ["MOVED", "ASK"])
async def test_cluster_redirections_still_work_without_network_replay(
    monkeypatch, cluster_store, redirect
):
    from redis.asyncio.cluster import ClusterNode
    from redis.exceptions import AskError, MovedError

    client = cluster_store._client
    key = cluster_store._keys.bucket(BucketRef("session", 3, 60_000))
    destination = ClusterNode("127.0.0.1", 6380, server_type="primary")
    calls = []

    async def execute(node, *args, **kwargs):
        calls.append((node.name, args))
        if node.port == 6379:
            error = MovedError if redirect == "MOVED" else AskError
            raise error(f"{client.keyslot(key)} 127.0.0.1:6380")
        return "OK" if args[0] == "ASKING" else 1

    source = configure_cluster_node(monkeypatch, client, key, execute)
    client.nodes_manager.nodes_cache[destination.name] = destination
    claim = await cluster_store.claim(
        BucketRef("session", 3, 60_000),
        BucketItem("item", encode_claim_score(1, 123)),
        attempt=1,
        started_at_seconds=200,
    )
    assert claim is not None
    assert claim.claim_score == encode_claim_score(1, 200)
    assert [name for name, _ in calls] == (
        [source.name, destination.name]
        if redirect == "MOVED"
        else [source.name, destination.name, destination.name]
    )
    assert [args[0] for _, args in calls] == (
        ["EVALSHA", "EVALSHA"] if redirect == "MOVED" else ["EVALSHA", "ASKING", "EVALSHA"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("close failed"), asyncio.CancelledError()])
async def test_client_reference_is_retained_until_close_succeeds(error):
    class Client:
        def __init__(self):
            self.aclose = AsyncMock(side_effect=[error, None])

    client = Client()
    timer_store = store(client)
    with pytest.raises(type(error)):
        await timer_store.close()
    assert timer_store._client is client
    await timer_store.close()
    assert timer_store._client is None
    assert client.aclose.await_count == 2


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
@pytest.mark.parametrize("delete_metadata", [True, False])
async def test_unregister_optionally_deletes_metadata(monkeypatch, delete_metadata):
    timer_store = store(FlakyClient([]))
    command = AsyncMock()
    single_key_command = AsyncMock()
    monkeypatch.setattr(timer_store, "_command", command)
    monkeypatch.setattr(timer_store, "_single_key_command", single_key_command)

    await timer_store.unregister_resource(3, "item", delete_metadata=delete_metadata)

    assert [call.args for call in command.await_args_list] == [
        ("zrem", timer_store._keys.source(3), "item")
    ]
    if delete_metadata:
        single_key_command.assert_awaited_once_with(
            "DEL",
            timer_store._keys.metadata("item"),
            retry="idempotent",
        )
    else:
        single_key_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_bucket_reset_uses_single_key_cluster_commands(monkeypatch):
    timer_store = store(FlakyClient([]))
    monkeypatch.setattr(timer_store, "assert_owner", AsyncMock())
    monkeypatch.setattr(timer_store, "read_progress", AsyncMock(return_value=0))
    single_key_command = AsyncMock()
    monkeypatch.setattr(timer_store, "_single_key_command", single_key_command)

    await timer_store.reset_unpublished_minute(
        OwnerToken(OwnerRole.SCHEDULER, 3, "owner", 999),
        minute_ms=60_000,
        expected_progress_ms=0,
        item_types=("session", "watch"),
    )

    assert [call.args[:2] for call in single_key_command.await_args_list] == [
        ("UNLINK", timer_store._keys.bucket(BucketRef("session", 3, 60_000))),
        ("UNLINK", timer_store._keys.bucket(BucketRef("watch", 3, 60_000))),
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
