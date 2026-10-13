import asyncio
import threading

import pytest

from openviking.scheduling.expiry.config import ExpiryConfig, ExpiryRedisConfig
from openviking.scheduling.expiry.models import StoreOperationError
from openviking.scheduling.expiry.runtime import ExpiryRuntime, RedisLoopExecutor
from openviking.scheduling.expiry.service import ExpiryRuntimeState


class Runner:
    has_pending = False

    async def wait_pending(self, timeout):
        return True


class Store:
    def __init__(self, finished):
        self.finished = finished
        self.close_calls = 0
        self.closed_while_running = False

    async def close(self):
        self.close_calls += 1
        self.closed_while_running |= not self.finished.is_set()


def runtime(io, store):
    result = object.__new__(ExpiryRuntime)
    result._config = ExpiryConfig.model_construct(
        shutdown_timeout_seconds=0.03,
        redis=ExpiryRedisConfig.model_construct(request_budget_seconds=0.03),
    )
    result._state = ExpiryRuntimeState(storage_ready=True, accepting_calls=True)
    result._stop = asyncio.Event()
    result._tasks = {}
    result._storage_started = True
    result._storage_closed = False
    result._store = store
    result._io = io
    result._shard_runner = Runner()
    return result


@pytest.mark.asyncio
async def test_runtime_preserves_dependencies_until_cancelled_remote_operation_really_exits():
    io = RedisLoopExecutor(max_inflight=1, request_budget_seconds=10.0)
    await io.start()
    thread = io._thread
    loop = io._loop
    started = threading.Event()
    cancelled = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    async def operation():
        started.set()
        try:
            while not release.is_set():
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    cancelled.set()
        finally:
            finished.set()

    store = Store(finished)
    timer_runtime = runtime(io, store)
    request = asyncio.create_task(io.run(operation))
    close_task = None
    try:
        assert await asyncio.to_thread(started.wait, 2.0)
        request.cancel()
        assert await asyncio.to_thread(cancelled.wait, 2.0)
        close_task = asyncio.create_task(timer_runtime.close())
        done, _ = await asyncio.wait({close_task}, timeout=0.5)
        assert close_task in done, "Shutdown must remain bounded even across the Redis loop"
        with pytest.raises(StoreOperationError) as error:
            close_task.result()
        assert error.value.code == "shard_not_stopped"
        assert store.close_calls == 0
        assert not store.closed_while_running
        assert timer_runtime._storage_started
        assert timer_runtime._state.storage_ready
        assert io._thread is thread
        assert io._loop is loop
        assert thread.is_alive()
        assert not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(request), timeout=2.0)
        assert await asyncio.to_thread(finished.wait, 2.0)
        await asyncio.wait_for(timer_runtime.close(), timeout=2.0)
        assert store.close_calls == 1
        assert not store.closed_while_running
        assert not thread.is_alive()
        assert io._thread is None
        assert not timer_runtime._storage_started
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), timeout=2.0)
        if close_task is not None:
            await asyncio.wait_for(asyncio.gather(close_task, return_exceptions=True), timeout=2.0)
        # Also join the captured thread: the old implementation incorrectly
        # discarded the only executor reference after its timed join expired.
        if thread.is_alive():
            assert await asyncio.to_thread(finished.wait, 2.0)
            await io.close(2.0)
            await asyncio.to_thread(thread.join, 2.0)
        if io._thread is not None:
            await io.close(2.0)


@pytest.mark.asyncio
async def test_executor_cancelled_wrapper_stays_pending_until_remote_task_exits():
    io = RedisLoopExecutor(max_inflight=1, request_budget_seconds=10.0)
    await io.start()
    started = threading.Event()
    cancelled = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    async def operation():
        started.set()
        try:
            while not release.is_set():
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    cancelled.set()
        finally:
            finished.set()

    request = asyncio.create_task(io.run(operation))
    try:
        assert await asyncio.to_thread(started.wait, 2.0)
        request.cancel()
        assert await asyncio.to_thread(cancelled.wait, 2.0)
        done, _ = await asyncio.wait({request}, timeout=0.03)
        assert request not in done, "A cancelled bridge is not proof of remote task termination"
        # Repeated cancellation during cleanup must not orphan remote work.
        request.cancel()
        done, _ = await asyncio.wait({request}, timeout=0.03)
        assert request not in done
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(request), timeout=2.0)
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), timeout=2.0)
        await io.close(2.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_executor_propagates_remote_result_or_exception_and_closes_cleanly(raises):
    io = RedisLoopExecutor(max_inflight=1, request_budget_seconds=1.0)
    await io.start()
    thread = io._thread

    async def operation():
        if raises:
            raise ValueError("remote error")
        return 42

    try:
        if raises:
            with pytest.raises(ValueError, match="remote error"):
                await io.run(operation)
        else:
            assert await io.run(operation) == 42
        await io.quiesce(0.1)
        with pytest.raises(StoreOperationError) as error:
            await io.run(operation)
        assert error.value.code == "not_started"
    finally:
        await io.close(2.0)
    assert not thread.is_alive()


@pytest.mark.asyncio
async def test_cleanup_runs_nested_io_without_reopening_admission_or_deadlocking_gate():
    io = RedisLoopExecutor(max_inflight=1, request_budget_seconds=1.0)
    await io.start()
    observed = []

    async def operation():
        observed.append(asyncio.get_running_loop())
        return 42

    async def cleanup():
        return await io.run(operation)

    try:
        await io.quiesce(0.1)
        assert await io.run_cleanup(cleanup, 1.0) == 42
        assert observed == [io._loop]
        with pytest.raises(StoreOperationError) as error:
            await io.run(operation)
        assert error.value.code == "not_started"
    finally:
        await io.close(2.0)


@pytest.mark.asyncio
async def test_resistant_storage_cleanup_is_bounded_and_retains_thread_for_retry():
    io = RedisLoopExecutor(max_inflight=1, request_budget_seconds=1.0)
    await io.start()
    thread = io._thread
    started = threading.Event()
    cancelled = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    class ResistantStore(Store):
        async def close(self):
            self.close_calls += 1
            if finished.is_set():
                return
            started.set()
            try:
                while not release.is_set():
                    try:
                        await asyncio.sleep(0.005)
                    except asyncio.CancelledError:
                        cancelled.set()
            finally:
                finished.set()

    store = ResistantStore(finished)
    timer_runtime = runtime(io, store)
    close_task = asyncio.create_task(timer_runtime.close())
    try:
        assert await asyncio.to_thread(started.wait, 2.0)
        assert await asyncio.to_thread(cancelled.wait, 2.0)
        done, _ = await asyncio.wait({close_task}, timeout=0.5)
        assert close_task in done
        with pytest.raises(StoreOperationError) as error:
            close_task.result()
        assert error.value.code == "shard_not_stopped"
        assert timer_runtime._storage_started
        assert timer_runtime._state.storage_ready
        assert io._thread is thread
        assert thread.is_alive()
        assert not finished.is_set()
        release.set()
        assert await asyncio.to_thread(finished.wait, 2.0)
        await asyncio.wait_for(timer_runtime.close(), timeout=2.0)
        assert not thread.is_alive()
        assert not timer_runtime._storage_started
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(close_task, return_exceptions=True), timeout=2.0)
        assert await asyncio.to_thread(finished.wait, 2.0)
        await io.close(2.0)


@pytest.mark.asyncio
async def test_request_timeout_never_reports_success_when_operation_suppresses_cancel():
    io = RedisLoopExecutor(max_inflight=1, request_budget_seconds=0.03)
    await io.start()
    cancelled = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    async def operation():
        try:
            while not release.is_set():
                try:
                    await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    cancelled.set()
            return "late success"
        finally:
            finished.set()

    request = asyncio.create_task(io.run(operation))
    try:
        assert await asyncio.to_thread(cancelled.wait, 2.0)
        done, _ = await asyncio.wait({request}, timeout=0.03)
        assert request not in done
        release.set()
        with pytest.raises(StoreOperationError) as error:
            await asyncio.wait_for(request, timeout=2.0)
        assert error.value.code == "redis_timeout"
        assert error.value.outcome_unknown
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), timeout=2.0)
        await io.close(2.0)


@pytest.mark.asyncio
async def test_executor_retains_loop_and_thread_if_timed_join_does_not_finish():
    class Loop:
        def __init__(self):
            self.stops = 0

        def is_running(self):
            return True

        def stop(self):
            self.stops += 1

        def call_soon_threadsafe(self, callback):
            callback()

    class Thread:
        def __init__(self):
            self.alive = True
            self.budgets = []

        def join(self, timeout):
            self.budgets.append(timeout)

        def is_alive(self):
            return self.alive

    io = RedisLoopExecutor(max_inflight=1, request_budget_seconds=1.0)
    loop, thread = Loop(), Thread()
    io._loop, io._thread, io._accepting = loop, thread, True
    with pytest.raises(StoreOperationError) as error:
        await io.close(0.01)
    assert error.value.code == "shard_not_stopped"
    assert io._loop is loop
    assert io._thread is thread
    assert io._accepting is False
    thread.alive = False
    await io.close(0.01)
    assert io._loop is None
    assert io._thread is None
    assert len(thread.budgets) == 2
    assert all(0.0 <= budget <= 0.01 for budget in thread.budgets)
    assert loop.stops == 1
