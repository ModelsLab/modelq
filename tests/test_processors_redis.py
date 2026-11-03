"""Opt-in live Redis checks, isolated from application queues by key prefixes.

Set MODELQ_TEST_REDIS_HOST, MODELQ_TEST_REDIS_PORT, MODELQ_TEST_REDIS_USERNAME,
and MODELQ_TEST_REDIS_PASSWORD. TLS defaults to false; enable with
MODELQ_TEST_REDIS_SSL=true. No production workers or maintenance loops are run.
"""

import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest
import redis

from modelq import ModelQ
from modelq.app.tasks import Task
from modelq.middleware import (
    ProcessorMiddleware,
    get_preprocessor_result,
    send_postprocessor,
)
from tests.test_processors import (
    test_concurrent_attempt_contexts_are_isolated,
    test_postprocessor_failure_is_observable_without_failing_task,
    test_precedes_task_and_background_post_starts_after_persistence,
    test_preprocessor_failure_prevents_task_body,
    test_processor_configuration_and_helper_errors,
    test_schema_processor_receives_validated_model,
    test_streaming_keeps_context_until_generator_finishes,
    test_timed_out_task_discards_staged_postprocessing,
    test_timeout_includes_preprocessing_and_prevents_late_task_start,
    test_unsuccessful_attempt_discards_postprocessing,
)


pytestmark = pytest.mark.skipif(
    not os.environ.get("MODELQ_TEST_REDIS_HOST"),
    reason="Live Redis tests require MODELQ_TEST_REDIS_HOST",
)


class _IsolatedRedis(redis.Redis):
    """Prefix every allowed command's key; reject unaccounted-for commands."""

    _SINGLE_KEY = {
        "GET", "SET", "HSET", "HGET", "HDEL", "SADD", "SREM",
        "ZADD", "ZREM", "RPUSH", "LPOP", "XADD", "XLEN", "EXPIRE",
        "LRANGE", "LREM", "LLEN",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.prefix = "modelq:processor-test:" + uuid.uuid4().hex + ":"
        self.test_keys = set()
        self.test_keys_lock = threading.Lock()

    def scope_command(self, *args):
        command = args[0].upper()
        if command in {"PING", "MULTI", "EXEC", "UNWATCH", "DISCARD"}:
            return args
        if command == "WATCH":
            with self.test_keys_lock:
                keys = tuple(self.prefix + str(key) for key in args[1:])
                self.test_keys.update(keys)
            return (args[0], *keys)
        if command == "BLMOVE":
            with self.test_keys_lock:
                keys = tuple(self.prefix + str(key) for key in args[1:3])
                self.test_keys.update(keys)
            return (args[0], *keys, *args[3:])
        if command not in self._SINGLE_KEY:
            raise AssertionError(f"Live test blocked unscoped Redis command: {command}")
        key = self.prefix + str(args[1])
        with self.test_keys_lock:
            self.test_keys.add(key)
        return (args[0], key, *args[2:])

    def execute_command(self, *args, **options):
        return super().execute_command(*self.scope_command(*args), **options)

    def pipeline(self, transaction=True, shard_hint=None):
        pipeline = _IsolatedPipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint
        )
        pipeline.owner = self
        return pipeline

    def cleanup(self):
        with self.test_keys_lock:
            keys = list(self.test_keys)
        assert all(key.startswith(self.prefix) for key in keys)
        if keys:
            # Bypass the single-key adapter only for deletion of our tracked keys.
            super().execute_command("DEL", *keys)
            assert super().execute_command("EXISTS", *keys) == 0


class _IsolatedPipeline(redis.client.Pipeline):
    def execute_command(self, *args, **options):
        return super().execute_command(*self.owner.scope_command(*args), **options)


@pytest.fixture
def mq():
    client = _IsolatedRedis(
        host=os.environ["MODELQ_TEST_REDIS_HOST"],
        port=int(os.environ.get("MODELQ_TEST_REDIS_PORT", "6379")),
        username=os.environ.get("MODELQ_TEST_REDIS_USERNAME", "default"),
        password=os.environ.get("MODELQ_TEST_REDIS_PASSWORD"),
        ssl=os.environ.get("MODELQ_TEST_REDIS_SSL", "false").lower() == "true",
        socket_connect_timeout=10,
        socket_timeout=10,
    )
    try:
        assert client.ping()
        queue = ModelQ(redis_client=client, server_id=client.prefix, silent=True)
        # Bound connection failures in tests instead of retrying indefinitely.
        queue.redis_client = client
        queue.log_task_error_to_file = Mock()
        yield queue
    finally:
        try:
            client.cleanup()
        finally:
            client.close()


def test_live_queue_roundtrip_and_postprocessor_redis_side_effect(mq):
    futures = []

    class Pre(ProcessorMiddleware):
        async def start(self, param):
            return param["a"] + param["b"]

    class Post(ProcessorMiddleware):
        async def start(self, param):
            mq.redis_client.set("postprocessor_output", json.dumps(param))
            return param

    @mq.task(preprocessor=Pre, postprocessor=Post)
    def add(param):
        result = get_preprocessor_result()
        futures.append(send_postprocessor({"sum": result}))
        return result

    submitted = add({"a": 2, "b": 3})
    assert mq.redis_client.lpop("ml_tasks") is None
    preprocessing = Task.from_dict(json.loads(mq.redis_client.lpop(mq.PREPROCESSING_QUEUE)))
    mq.process_preprocessor(preprocessing)
    # Claim only from our isolated queue, then execute the normal worker path.
    queued = Task.from_dict(json.loads(mq.redis_client.lpop("ml_tasks")))
    assert queued.task_id == submitted.task_id
    mq.process_task(queued)
    assert submitted.get_result(mq.redis_client, timeout=5) == 5
    assert futures[0].result(timeout=5) == {"sum": 5}
    assert json.loads(mq.redis_client.get("postprocessor_output")) == {"sum": 5}


def test_live_preprocessing_finishes_during_execution_backlog(mq):
    busy, release, prepared, body = (threading.Event() for _ in range(4))
    events = []

    @mq.task()
    def slow():
        events.append("first task started; execution worker busy")
        busy.set()
        assert release.wait(10)

    class Pre(ProcessorMiddleware):
        async def start(self, param):
            result = param["a"] + param["b"]
            events.append(f"second task preprocessor finished: {result}")
            prepared.set()
            return result

    @mq.task(preprocessor=Pre)
    def add(param):
        body.set()
        events.append(f"second task body started; preprocessor result: {get_preprocessor_result()}")
        return get_preprocessor_result()

    with _running_isolated_workers(mq):
        try:
            first = slow()
            assert busy.wait(5)
            second = add({"a": 20, "b": 22})
            assert prepared.wait(5)
            state = _wait_for_prepared(mq, second)
            assert state["status"] == "queued"
            assert state["started_at"] is None
            assert state["payload"][mq.PREPROCESSOR_RESULT] == 42
            assert not body.is_set()
            events.append("first task still blocked; second task body started: False")
            release.set()
            assert second.get_result(mq.redis_client, timeout=5) == 42
            assert first.get_result(mq.redis_client, timeout=5) == "None"
            events.append("second task result: 42")
        finally:
            release.set()
    print("\nHIGH TRAFFIC (one execution worker occupied)\n" + "\n".join(events))


class _StopTestWorker(BaseException):
    pass


@contextmanager
def _running_isolated_workers(mq, execution_polled=None):
    """Run real worker loops; stop them before the fixture deletes test keys.

    Maintenance is disabled so the test only runs task and preprocessing workers.
    Every Redis command still goes through the fail-closed namespace adapter.
    """
    stop = threading.Event()
    original_claim = mq._claim_task
    original_thread = threading.Thread

    def claim(inflight_key, queue_name="ml_tasks"):
        if stop.is_set():
            raise _StopTestWorker()
        if queue_name == "ml_tasks" and execution_polled is not None:
            execution_polled.set()
        return original_claim(inflight_key, queue_name)

    def thread_factory(*args, **kwargs):
        target = kwargs.pop("target")

        def run(*args, **kwargs):
            try:
                target(*args, **kwargs)
            except _StopTestWorker:
                pass

        return original_thread(*args, target=run, **kwargs)

    mq._claim_task = claim
    mq.BLPOP_TIMEOUT = 0.1
    mq.recover_abandoned_inflight_tasks = Mock()
    mq.requeue_delayed_tasks = Mock()
    mq._heartbeat_loop = Mock()
    mq._pruning_loop = Mock()
    try:
        with patch("threading.Thread", side_effect=thread_factory):
            mq.start_workers(no_of_workers=1, no_of_preprocessors=1)
        yield
    finally:
        stop.set()
        for thread in mq.worker_threads:
            thread.join(5)
            assert not thread.is_alive(), "live test worker did not stop"


def _wait_for_prepared(mq, task):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        blob = mq.redis_client.get(f"task:{task.task_id}")
        state = json.loads(blob) if blob else {}
        if mq.PREPROCESSOR_RESULT in state.get("payload", {}):
            return state
        time.sleep(0.01)
    pytest.fail("preprocessor result was not persisted")


def test_live_idle_execution_worker_cannot_start_before_preprocessor_finishes(mq):
    entered, release, body, polled = (threading.Event() for _ in range(4))
    events = []

    class Pre(ProcessorMiddleware):
        async def start(self, param):
            events.append("preprocessor started; held until test releases it")
            entered.set()
            assert release.wait(10)
            result = param["a"] + param["b"]
            events.append(f"preprocessor finished: {result}")
            return result

    @mq.task(preprocessor=Pre)
    def add(param):
        result = get_preprocessor_result()
        events.append(f"task body started; preprocessor result: {result}")
        body.set()
        return result

    with _running_isolated_workers(mq, execution_polled=polled):
        try:
            task = add({"a": 2, "b": 3})
            assert entered.wait(5)
            polled.clear()
            assert polled.wait(5), "idle execution worker did not poll during preprocessing"
            assert mq.redis_client.llen("ml_tasks") == 0
            assert not body.is_set()
            events.append("idle execution worker polled; execution queue length: 0")
            events.append("preprocessor still blocked; task body started: False")
            release.set()
            assert task.get_result(mq.redis_client, timeout=5) == 5
            assert body.is_set()
            assert events.index("preprocessor finished: 5") < events.index(
                "task body started; preprocessor result: 5"
            )
            events.append("task result: 5")
        finally:
            release.set()
    print("\nLOW TRAFFIC (execution worker idle)\n" + "\n".join(events))
