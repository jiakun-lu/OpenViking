# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Exercise expiry selection through the core's actual initialization path.

Keep these tests self-contained so release jobs can also run them against an
installed wheel outside the checkout, with pytest's conftest loading disabled.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import openviking.scheduling.expiry.runtime as expiry_runtime
import openviking.service.core as core
from openviking.scheduling.expiry.config import ExpiryConfig, ExpiryRedisConfig
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils.config.open_viking_config import OpenVikingConfig


@pytest.fixture
def startup(monkeypatch):
    config = OpenVikingConfig.from_dict({})
    service = core.OpenVikingService.__new__(core.OpenVikingService)
    service._config = config
    service._initialized = False
    service._user = UserIdentifier.the_default_user()
    service._encryptor = None
    service._expiry_runtime = None
    service._compile_scheduler = None
    service._session_auto_commit_scheduler = None
    monkeypatch.setattr(service, "_ensure_data_dir_lock_acquired", Mock())
    monkeypatch.setattr(service, "_init_runtime_config_manager", AsyncMock())
    for name in (
        "agfs_client",
        "vikingdb_manager",
        "vlm_resolver",
        "embedding_provider",
        "vector_config_resolver",
        "runtime_config_manager",
        "uri_mutation_coordinator",
        "fs_service",
        "pack_service",
        "search_service",
        "resource_service",
        "session_service",
        "resource_memory_link_service",
        "debug_service",
        "agent_evolution_service",
        "compile_service",
    ):
        setattr(service, f"_{name}", Mock())
    service._external_task_service = Mock(restore_tasks=AsyncMock())
    service._queue_manager = Mock(
        prepare_task_tracking=AsyncMock(), get_queued_tasks=AsyncMock(return_value=[])
    )
    monkeypatch.setattr(core, "get_openviking_config", lambda: config)
    monkeypatch.setattr(core, "get_task_tracker", Mock(return_value=Mock(load=AsyncMock())))
    monkeypatch.setattr(core, "init_context_collection", AsyncMock())
    monkeypatch.setattr(core, "init_viking_fs", Mock())
    for name in (
        "ResourceProcessor",
        "UserPrivacyConfigService",
        "PrivacySanitizer",
        "SkillProcessor",
        "create_session_compressor",
        "CompileStateStore",
        "CompileLifecycle",
        "build_credential_encryptor",
        "AddResourceProcessor",
        "ReindexProcessor",
        "ContentWriteProcessor",
        "SessionCommitProcessor",
        "ExternalTaskProcessor",
    ):
        monkeypatch.setattr(core, name, Mock())
    monkeypatch.setattr(
        core,
        "DirectoryInitializer",
        Mock(return_value=Mock(initialize_account_workspace=AsyncMock(return_value=(1, 1)))),
    )
    watch = Mock(start=AsyncMock())
    local = Mock(start=AsyncMock())
    compile_scheduler = Mock(start=AsyncMock())
    runtime = Mock(start_storage=AsyncMock(), start_roles=AsyncMock(), close=AsyncMock())
    runtime_factory = Mock(return_value=runtime)
    monkeypatch.setattr(core, "WatchScheduler", Mock(return_value=watch))
    monkeypatch.setattr(core, "SessionAutoCommitScheduler", Mock(return_value=local))
    monkeypatch.setattr(core, "CompileScheduler", Mock(return_value=compile_scheduler))
    monkeypatch.setattr(expiry_runtime, "ExpiryRuntime", runtime_factory)
    config.enable_watch_scheduler = True
    config.pdf.strategy = "local"
    return SimpleNamespace(
        service=service,
        config=config,
        watch=watch,
        local=local,
        compile=compile_scheduler,
        runtime=runtime,
        runtime_factory=runtime_factory,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,backend", [(False, "local"), (False, "redis"), (True, "local")])
@pytest.mark.parametrize("legacy_config", [False, True])
async def test_initialize_without_expiry_consumers(startup, enabled, backend, legacy_config):
    idle = startup.config.memory.session_auto_commit
    idle.enabled, idle.idle_backend = enabled, backend
    if legacy_config:
        # Model the missing attributes in configs predating distributed scheduling.
        del startup.config.expiry
        del startup.config.compile_api.scheduler_enabled
        if backend == "local":
            del idle.idle_backend

    await startup.service.initialize()

    assert startup.service._initialized
    assert startup.local.start.await_count == int(enabled)
    startup.runtime_factory.assert_not_called()
    startup.compile.start.assert_not_awaited()
    startup.service._queue_manager.start.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("consumer", ["session", "compile", "both"])
@pytest.mark.parametrize("missing", [True, False], ids=["missing", "invalid"])
async def test_expiry_config_failure_precedes_worker_start(startup, consumer, missing):
    idle = startup.config.memory.session_auto_commit
    # With Compile alone enabled, even the local session scheduler must not start
    # before the invalid expiry configuration has been rejected.
    idle.enabled = True
    idle.idle_backend = "redis" if consumer in {"session", "both"} else "local"
    startup.config.compile_api.scheduler_enabled = consumer in {"compile", "both"}
    expected_error: type[Exception]
    if missing:
        del startup.config.expiry
        expected_error, message = RuntimeError, "Reinstall a complete OpenViking version"
    else:
        startup.config.expiry = ExpiryConfig(service_name="startup-test")
        expected_error, message = ValueError, r"expiry\.redis\.endpoints is required"

    with pytest.raises(expected_error, match=message):
        await startup.service.initialize()

    assert not startup.service._initialized
    startup.runtime_factory.assert_not_called()
    startup.local.start.assert_not_awaited()
    startup.compile.start.assert_not_awaited()
    startup.watch.start.assert_not_awaited()
    startup.service._queue_manager.start.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("consumer", ["session", "compile", "both"])
async def test_initialize_starts_configured_expiry_consumers(startup, consumer):
    idle = startup.config.memory.session_auto_commit
    idle.enabled = consumer in {"session", "both"}
    idle.idle_backend = "redis"
    startup.config.compile_api.scheduler_enabled = consumer in {"compile", "both"}
    startup.config.expiry = ExpiryConfig(
        service_name="startup-test",
        redis=ExpiryRedisConfig(endpoints=["redis://127.0.0.1:6379"]),
    )

    await startup.service.initialize()

    assert startup.service._initialized
    startup.runtime_factory.assert_called_once()
    assert startup.runtime_factory.call_args.args[0] is startup.config.expiry
    roles = startup.runtime_factory.call_args.kwargs["roles"]
    assert roles.report and roles.consume and not roles.publish
    startup.runtime.start_storage.assert_awaited_once()
    startup.runtime.start_roles.assert_awaited_once()
    startup.local.start.assert_not_awaited()
    assert startup.service._session_service.configure_expiry.call_count == int(idle.enabled)
    if consumer in {"compile", "both"}:
        startup.compile.install_expiry.assert_called_once_with(startup.runtime.scheduler)
        startup.compile.start.assert_awaited_once()
    else:
        startup.compile.start.assert_not_awaited()
