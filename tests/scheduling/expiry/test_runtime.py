import asyncio
import threading

import pytest

from openviking.scheduling.expiry import runtime as runtime_module
from openviking.scheduling.expiry.config import ExpiryConfig, ExpiryRedisConfig
from openviking.scheduling.expiry.models import StoreOperationError
from openviking.scheduling.expiry.runtime import ExpiryRuntime, ExpiryRuntimeRoles
from openviking.scheduling.expiry.service import ExpiryRuntimeState
from openviking.service.core import OpenVikingService


class Store:
    def __init__(self):
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1


class IO:
    def __init__(self):
        self.close_budgets = []

    async def quiesce(self, timeout_seconds):
        pass

    async def run_cleanup(self, operation, timeout_seconds):
        return await operation()

    async def close(self, timeout_seconds):
        self.close_budgets.append(timeout_seconds)


class Runner:
    def __init__(self, *, pending=False):
        self.settled = asyncio.Event()
        if not pending:
            self.settled.set()
        self.wait_budgets = []

    @property
    def has_pending(self):
        return not self.settled.is_set()

    async def wait_pending(self, timeout):
        self.wait_budgets.append(timeout)
        if not self.has_pending:
            return True
        waiter = asyncio.create_task(self.settled.wait())
        try:
            finished, _ = await asyncio.wait({waiter}, timeout=timeout)
            return waiter in finished
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)


def runtime(*, pending=False):
    # Exercise the actual shutdown helpers without constructing Redis clients,
    # starting an I/O thread, or replacing the helpers under test.
    result = object.__new__(ExpiryRuntime)
    result._config = ExpiryConfig.model_construct(
        shutdown_timeout_seconds=0.02,
        redis=ExpiryRedisConfig.model_construct(request_budget_seconds=0.03),
    )
    result._state = ExpiryRuntimeState()
    result._state.accepting_calls = True
    result._state.storage_ready = True
    result._stop = asyncio.Event()
    result._tasks = {}
    result._storage_started = True
    result._storage_closed = False
    result._storage_owned = True
    result._store_open_attempted = True
    result._storage_start_task = None
    result._store = Store()
    result._io = IO()
    result._shard_runner = Runner(pending=pending)
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["io", "open", "namespace"])
async def test_cancelled_storage_start_closes_partial_resources(stage):
    timer_runtime = ExpiryRuntime(
        ExpiryConfig(service_name="startup-test"), roles=ExpiryRuntimeRoles()
    )
    entered = asyncio.Event()
    original_start = timer_runtime._io.start

    class StartupStore(Store):
        async def open(self):
            if stage == "open":
                entered.set()
                await asyncio.Event().wait()

        async def ensure_namespace(self, spec):
            if stage == "namespace":
                entered.set()
                await asyncio.Event().wait()

    async def io_start():
        await original_start()
        if stage == "io":
            entered.set()
            await asyncio.Event().wait()

    timer_runtime._store = StartupStore()
    timer_runtime._io.start = io_start
    start_task = asyncio.create_task(timer_runtime.start_storage())
    thread = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        thread = timer_runtime._io._thread
        start_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(start_task, timeout=2.0)
        assert not timer_runtime._storage_started
        assert not timer_runtime._storage_owned
        assert timer_runtime._storage_start_task is None
        assert not timer_runtime._state.storage_ready
        assert thread is not None and not thread.is_alive()
        assert timer_runtime._store.close_calls == (0 if stage == "io" else 1)
        await timer_runtime.close()
        assert timer_runtime._store.close_calls == (0 if stage == "io" else 1)
    finally:
        await asyncio.gather(start_task, return_exceptions=True)
        await timer_runtime.close()


@pytest.mark.asyncio
async def test_cancelled_start_before_io_loop_is_ready_can_retry_close():
    timer_runtime = ExpiryRuntime(
        ExpiryConfig(service_name="startup-test", shutdown_timeout_seconds=0.03),
        roles=ExpiryRuntimeRoles(),
    )
    started = threading.Event()
    release = threading.Event()
    original_thread_main = timer_runtime._io._thread_main

    def delayed_thread_main():
        started.set()
        release.wait()
        original_thread_main()

    timer_runtime._io._thread_main = delayed_thread_main
    start_task = asyncio.create_task(timer_runtime.start_storage())
    try:
        assert await asyncio.to_thread(started.wait, 2.0)
        thread = timer_runtime._io._thread
        start_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(start_task, timeout=2.0)
        assert timer_runtime._storage_owned
        assert not timer_runtime._storage_started
        assert thread is not None and thread.is_alive()
        release.set()
        await asyncio.wait_for(timer_runtime.close(), timeout=2.0)
        assert not timer_runtime._storage_owned
        assert not thread.is_alive()
    finally:
        release.set()
        await asyncio.gather(start_task, return_exceptions=True)
        await timer_runtime.close()


@pytest.mark.asyncio
async def test_resistant_storage_start_is_retained_for_close_retry():
    timer_runtime = ExpiryRuntime(
        ExpiryConfig(service_name="startup-test", shutdown_timeout_seconds=0.02),
        roles=ExpiryRuntimeRoles(),
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    class StartupStore(Store):
        async def open(self):
            entered.set()
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    pass

        async def ensure_namespace(self, spec):
            pass

    timer_runtime._store = StartupStore()
    start_task = asyncio.create_task(timer_runtime.start_storage())
    try:
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        thread = timer_runtime._io._thread
        start_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(start_task, timeout=2.0)
        assert timer_runtime._storage_owned
        assert timer_runtime._storage_start_task is not None
        assert not timer_runtime._storage_start_task.done()
        assert timer_runtime._store.close_calls == 0
        with pytest.raises(StoreOperationError, match="Previous expiry storage startup"):
            await timer_runtime.start_storage()
        release.set()
        await asyncio.wait_for(timer_runtime.close(), timeout=2.0)
        assert timer_runtime._store.close_calls == 1
        assert not timer_runtime._storage_owned
        assert thread is not None and not thread.is_alive()
    finally:
        release.set()
        await asyncio.gather(start_task, return_exceptions=True)
        await timer_runtime.close()


@pytest.mark.asyncio
async def test_failed_start_preserves_failure_and_cleans_storage():
    timer_runtime = ExpiryRuntime(
        ExpiryConfig(service_name="startup-test"), roles=ExpiryRuntimeRoles()
    )
    failure = RuntimeError("namespace failed")

    class StartupStore(Store):
        async def open(self):
            pass

        async def ensure_namespace(self, spec):
            raise failure

    timer_runtime._store = StartupStore()
    with pytest.raises(RuntimeError) as error:
        await timer_runtime.start_storage()
    assert error.value is failure
    assert timer_runtime._store.close_calls == 1
    assert not timer_runtime._storage_owned
    assert timer_runtime._io._thread is None


@pytest.mark.asyncio
async def test_storage_can_start_again_after_partial_start_cleanup():
    timer_runtime = ExpiryRuntime(
        ExpiryConfig(service_name="startup-test"), roles=ExpiryRuntimeRoles()
    )

    class StartupStore(Store):
        fail_open = True

        async def open(self):
            if self.fail_open:
                raise RuntimeError("open failed")

        async def ensure_namespace(self, spec):
            pass

    timer_runtime._store = StartupStore()
    with pytest.raises(RuntimeError, match="open failed"):
        await timer_runtime.start_storage()
    timer_runtime._store.fail_open = False
    try:
        await timer_runtime.start_storage()
        assert timer_runtime._storage_started
        assert timer_runtime._storage_owned
        assert timer_runtime._state.storage_ready
        await timer_runtime.start_storage()
    finally:
        await timer_runtime.close()
    assert timer_runtime._store.close_calls == 2
    assert not timer_runtime._storage_owned


@pytest.mark.asyncio
async def test_cancelled_start_with_failed_cleanup_retains_resources_for_retry():
    timer_runtime = ExpiryRuntime(
        ExpiryConfig(service_name="startup-test"), roles=ExpiryRuntimeRoles()
    )
    entered = asyncio.Event()

    class StartupStore(Store):
        async def open(self):
            entered.set()
            await asyncio.Event().wait()

        async def close(self):
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("cleanup failed")

    timer_runtime._store = StartupStore()
    start_task = asyncio.create_task(timer_runtime.start_storage())
    try:
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        thread = timer_runtime._io._thread
        start_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(start_task, timeout=2.0)
        assert timer_runtime._storage_owned
        assert not timer_runtime._storage_closed
        assert thread is not None and thread.is_alive()
        await timer_runtime.close()
        assert timer_runtime._store.close_calls == 2
        assert not timer_runtime._storage_owned
        assert not thread.is_alive()
    finally:
        await asyncio.gather(start_task, return_exceptions=True)
        await timer_runtime.close()


def test_consumer_role_requires_reporting():
    with pytest.raises(ValueError, match="requires reporting"):
        ExpiryRuntimeRoles(consume=True)


@pytest.mark.asyncio
async def test_start_roles_creates_one_callback_task_per_item_type(monkeypatch):
    created = []

    class Consumer:
        def __init__(self, store, runner, item_type, binding, config, metrics):
            self.item_type = item_type
            created.append(item_type)

        async def run(self, stop):
            await stop.wait()

    class Registry:
        def freeze(self):
            return {"session": object(), "watch": object()}

    timer_runtime = object.__new__(ExpiryRuntime)
    timer_runtime._storage_started = True
    timer_runtime._state = ExpiryRuntimeState()
    timer_runtime._config = ExpiryConfig.model_construct()
    timer_runtime._roles = ExpiryRuntimeRoles(report=True, consume=True)
    timer_runtime._registry = Registry()
    timer_runtime._stop = asyncio.Event()
    timer_runtime._store = object()
    timer_runtime._shard_runner = object()
    timer_runtime._metrics = object()
    timer_runtime._publisher = None
    timer_runtime._consumers = {}
    spawned = {}

    def spawn_role(name, operation):
        spawned[name] = operation

    timer_runtime._spawn_role = spawn_role
    monkeypatch.setattr(runtime_module, "ExpiryConsumer", Consumer)

    await timer_runtime.start_roles()

    assert created == ["session", "watch"]
    assert set(timer_runtime._consumers) == {"session", "watch"}
    assert set(spawned) == {"callback:session", "callback:watch"}
    for operation in spawned.values():
        operation.close()


@pytest.mark.asyncio
async def test_start_roles_creates_publisher_only_when_selected(monkeypatch):
    class Publisher:
        def __init__(self, store, runner, config, metrics):
            pass

        async def run(self, stop):
            await stop.wait()

    class Registry:
        def freeze(self):
            return {}

    timer_runtime = object.__new__(ExpiryRuntime)
    timer_runtime._storage_started = True
    timer_runtime._state = ExpiryRuntimeState()
    timer_runtime._config = ExpiryConfig.model_construct()
    timer_runtime._roles = ExpiryRuntimeRoles(publish=True)
    timer_runtime._registry = Registry()
    timer_runtime._stop = asyncio.Event()
    timer_runtime._store = object()
    timer_runtime._shard_runner = object()
    timer_runtime._metrics = object()
    timer_runtime._publisher = None
    timer_runtime._consumers = {}
    spawned = {}

    def spawn_role(name, operation):
        spawned[name] = operation

    timer_runtime._spawn_role = spawn_role
    monkeypatch.setattr(runtime_module, "ExpiryPublisher", Publisher)

    await timer_runtime.start_roles()

    assert timer_runtime._publisher is not None
    assert set(spawned) == {"publisher"}
    spawned["publisher"].close()


@pytest.mark.asyncio
async def test_stop_roles_clears_cooperative_tasks_before_closing_dependencies():
    timer_runtime = runtime()
    started = asyncio.Event()
    finished = asyncio.Event()

    async def role():
        started.set()
        await timer_runtime._stop.wait()
        finished.set()

    role_task = asyncio.create_task(role())
    timer_runtime._tasks["callback"] = role_task
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)
        await asyncio.wait_for(timer_runtime.stop_roles(), timeout=2.0)
        assert finished.is_set()
        assert role_task.done()
        assert timer_runtime._tasks == {}
        assert timer_runtime._state.accepting_calls is False
        assert timer_runtime._store.close_calls == 0
        assert timer_runtime._io.close_budgets == []
        await asyncio.wait_for(timer_runtime.close(), timeout=2.0)
        assert timer_runtime._store.close_calls == 1
        assert timer_runtime._io.close_budgets == [0.02]
        assert timer_runtime._storage_started is False
        assert timer_runtime._state.storage_ready is False
    finally:
        timer_runtime._stop.set()
        await asyncio.wait_for(asyncio.gather(role_task, return_exceptions=True), timeout=2.0)


@pytest.mark.asyncio
async def test_resistant_role_has_bounded_stop_and_retains_tasks_and_open_dependencies():
    timer_runtime = runtime()
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release_role = asyncio.Event()

    async def role():
        started.set()
        while not release_role.is_set():
            try:
                await release_role.wait()
            except asyncio.CancelledError:
                cancelled.set()

    role_task = asyncio.create_task(role())
    timer_runtime._tasks["callback"] = role_task
    stop_task = None
    close_task = None
    try:
        await asyncio.wait_for(started.wait(), timeout=2.0)
        stop_task = asyncio.create_task(timer_runtime.stop_roles())
        await asyncio.wait_for(cancelled.wait(), timeout=2.0)
        finished, _ = await asyncio.wait({stop_task}, timeout=0.3)
        assert stop_task in finished, (
            "Role shutdown must not join cancellation resistance indefinitely"
        )
        with pytest.raises(StoreOperationError) as error:
            stop_task.result()
        assert error.value.code == "shard_not_stopped"
        assert timer_runtime._tasks == {"callback": role_task}
        assert not role_task.done()
        assert timer_runtime._state.accepting_calls is False
        close_task = asyncio.create_task(timer_runtime.close())
        finished, _ = await asyncio.wait({close_task}, timeout=0.3)
        assert close_task in finished
        with pytest.raises(StoreOperationError) as error:
            close_task.result()
        assert error.value.code == "shard_not_stopped"
        assert timer_runtime._tasks == {"callback": role_task}
        assert timer_runtime._store.close_calls == 0
        assert timer_runtime._io.close_budgets == []
        assert timer_runtime._storage_started is True
        assert timer_runtime._state.storage_ready is True
    finally:
        # Release resistant work before joining shutdown tasks; a cancellation
        # watchdog must never become an unbounded join of the resistant role.
        release_role.set()
        tasks = [task for task in (role_task, stop_task, close_task) if task is not None]
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2.0)
        await asyncio.wait_for(timer_runtime.close(), timeout=2.0)
    assert timer_runtime._tasks == {}
    assert timer_runtime._store.close_calls == 1
    assert timer_runtime._io.close_budgets == [0.02]


@pytest.mark.asyncio
async def test_runner_pending_blocks_close_after_role_task_has_exited():
    timer_runtime = runtime(pending=True)

    async def role():
        return None

    role_task = asyncio.create_task(role())
    await role_task
    timer_runtime._tasks["callback"] = role_task
    close_task = asyncio.create_task(timer_runtime.close())
    try:
        finished, _ = await asyncio.wait({close_task}, timeout=0.3)
        assert close_task in finished
        with pytest.raises(StoreOperationError) as error:
            close_task.result()
        assert error.value.code == "shard_not_stopped"
        assert timer_runtime._tasks == {}
        assert timer_runtime._shard_runner.has_pending is True
        assert timer_runtime._store.close_calls == 0
        assert timer_runtime._io.close_budgets == []
        assert timer_runtime._storage_started is True
        assert timer_runtime._state.storage_ready is True
    finally:
        timer_runtime._shard_runner.settled.set()
        await asyncio.wait_for(asyncio.gather(close_task, return_exceptions=True), timeout=2.0)
        await asyncio.wait_for(timer_runtime.close(), timeout=2.0)
    assert timer_runtime._store.close_calls == 1
    assert timer_runtime._io.close_budgets == [0.02]
    assert timer_runtime._storage_started is False


@pytest.mark.asyncio
async def test_runtime_retries_thread_join_without_resubmitting_closed_storage_cleanup():
    timer_runtime = runtime()

    class SlowThreadIO(IO):
        def __init__(self):
            super().__init__()
            self.stopping = False

        async def run_cleanup(self, operation, timeout_seconds):
            if self.stopping:
                raise StoreOperationError("not_started", "Redis loop already stopping")
            return await operation()

        async def close(self, timeout_seconds):
            self.close_budgets.append(timeout_seconds)
            if not self.stopping:
                self.stopping = True
                raise StoreOperationError("shard_not_stopped", "I/O thread still exiting")

    timer_runtime._io = SlowThreadIO()
    with pytest.raises(StoreOperationError) as error:
        await timer_runtime.close()
    assert error.value.code == "shard_not_stopped"
    assert timer_runtime._storage_started
    assert timer_runtime._state.storage_ready
    assert timer_runtime._store.close_calls == 1
    await timer_runtime.close()
    assert timer_runtime._store.close_calls == 1
    assert timer_runtime._io.close_budgets == [0.02, 0.02]
    assert not timer_runtime._storage_started
    assert not timer_runtime._state.storage_ready


class SessionService:
    def __init__(self):
        self.adapter = object()
        self.adapter_updates = []

    def set_session_expiry_adapter(self, adapter):
        self.adapter_updates.append(adapter)
        self.adapter = adapter


class Runtime:
    def __init__(self, *, error=None):
        self.error = error
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1
        if self.error is not None:
            raise self.error


def service(expiry_runtime):
    result = object.__new__(OpenVikingService)
    result._expiry_runtime = expiry_runtime
    result._session_service = SessionService()
    return result


@pytest.mark.asyncio
async def test_core_retains_runtime_and_adapter_when_timer_work_has_not_stopped():
    shutdown_error = StoreOperationError("shard_not_stopped", "callback still running")
    expiry_runtime = Runtime(error=shutdown_error)
    core = service(expiry_runtime)
    original_adapter = core._session_service.adapter
    with pytest.raises(StoreOperationError) as error:
        await core._stop_expiry()
    assert error.value is shutdown_error
    assert expiry_runtime.close_calls == 1
    assert core._expiry_runtime is expiry_runtime
    assert core._session_service.adapter is original_adapter
    assert core._session_service.adapter_updates == []
    expiry_runtime.error = None
    await core._stop_expiry()
    assert expiry_runtime.close_calls == 2
    assert core._expiry_runtime is None
    assert core._session_service.adapter is None
    assert core._session_service.adapter_updates == [None]


@pytest.mark.asyncio
async def test_core_clears_runtime_and_detaches_adapter_after_successful_close():
    expiry_runtime = Runtime()
    core = service(expiry_runtime)
    await core._stop_expiry()
    assert expiry_runtime.close_calls == 1
    assert core._expiry_runtime is None
    assert core._session_service.adapter is None
    assert core._session_service.adapter_updates == [None]
