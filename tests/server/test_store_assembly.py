# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import ssl
from types import SimpleNamespace

import pytest

from openviking.server.account_stores.redis_client import create_redis_client
from openviking.server.api_keys.legacy import FileStore
from openviking.server.config import AccountStoreConfig
from openviking.server.store_assembly import (
    build_account_store,
    build_api_key_manager,
)
from openviking_cli.exceptions import InvalidArgumentError
from tests.server.account_store_fakes import InMemoryAGFS


async def test_account_store_connection_configuration():
    with pytest.raises(ValueError, match="account_store.provider must be one of"):
        AccountStoreConfig(provider="missing")
    with pytest.raises(InvalidArgumentError, match="Unknown account store provider"):
        build_account_store(
            viking_fs=SimpleNamespace(agfs=InMemoryAGFS()),
            api_key_hashing_enabled=False,
            account_store_provider="missing",
            account_store_params=None,
            account_store_watch_enabled=False,
            account_store_watch_interval_seconds=30,
        )

    redis_connection = pytest.importorskip("redis.asyncio.connection")
    handle = create_redis_client({"mode": "standalone", "endpoints": ["rediss://127.0.0.1:6379"]})
    try:
        # Constructing only the pool would miss unsupported connection arguments.
        connection = handle.client.connection_pool.make_connection()
        assert isinstance(connection, redis_connection.SSLConnection)
        assert connection.ssl_context.get().verify_mode == ssl.CERT_REQUIRED
    finally:
        await handle.close()


@pytest.mark.parametrize("injected", [False, True])
async def test_assembled_manager_supports_account_authentication(injected):
    account_store = FileStore(SimpleNamespace(agfs=InMemoryAGFS()))
    manager = build_api_key_manager(
        "root",
        None if injected else SimpleNamespace(agfs=InMemoryAGFS()),
        account_store_provider=AccountStoreConfig(provider=" FILE ").provider,
        account_store=account_store if injected else None,
    )
    await manager.load()
    try:
        key = await manager.create_account("acme", "alice")
        assert (await manager.resolve_identity(key)).user_id == "alice"
    finally:
        await manager.close()
