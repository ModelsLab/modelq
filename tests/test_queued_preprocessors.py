"""Preprocessing advances while execution workers are occupied."""

import json
import threading
import time
from unittest.mock import Mock

import fakeredis
import pytest

from modelq import ModelQ
from modelq.app.tasks import Task
from modelq.exceptions import RetryTaskException, TaskProcessingError
from modelq.middleware import ProcessorMiddleware, get_preprocessor_result


@pytest.fixture
def mq():
    queue = ModelQ(redis_client=fakeredis.FakeStrictRedis(), server_id="queued-pre-test")
    queue.log_task_error_to_file = Mock()
    return queue


def _claim_preprocessor(mq):
    key = mq._inflight_key("preprocessor_test")
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, key)
    raw = mq._claim_task(key, mq.PREPROCESSING_QUEUE)
    assert raw is not None
    return Task.from_dict(json.loads(raw)), key, raw


def test_preprocessing_runs_while_only_execution_worker_is_busy(mq):
    busy, release, prepared, body = (threading.Event() for _ in range(4))

    @mq.task()
    def slow():
        busy.set()
        assert release.wait(5)

    class Pre(ProcessorMiddleware):
        async def start(self, param):
            prepared.set()
            return {"sum": param["a"] + param["b"]}

    @mq.task(preprocessor=Pre)
    def add(param):
        body.set()
        return get_preprocessor_result()["sum"]

    mq.BLPOP_TIMEOUT = 1
    mq.start_workers(no_of_workers=1, no_of_preprocessors=1)
    try:
        slow()
        assert busy.wait(5)
        task = add({"a": 2, "b": 3})
        assert prepared.wait(5), "preprocessing must not wait for execution capacity"
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = json.loads(mq.redis_client.get(f"task:{task.task_id}"))
            if mq.PREPROCESSOR_RESULT in state["payload"]:
                break
            time.sleep(0.01)
        assert state["payload"][mq.PREPROCESSOR_RESULT] == {"sum": 5}
        assert state["status"] == "queued"
        assert state["started_at"] is None
        assert not body.is_set()
        assert mq.redis_client.zscore("queued_requests", task.task_id) is not None
        release.set()
        assert task.get_result(mq.redis_client, timeout=5) == 5
        assert body.is_set()
    finally:
        release.set()
        mq.worker_healthy = False


def test_preprocessing_result_survives_worker_change_and_is_not_repeated(mq):
    called = Mock()

    class Pre(ProcessorMiddleware):
        def start(self, value):
            called()
            return {"value": value * 2}

    @mq.task(preprocessor=Pre)
    def compute(value):
        return get_preprocessor_result()

    submitted = compute(4)
    assert mq.redis_client.llen("ml_tasks") == 0
    claimed, key, raw = _claim_preprocessor(mq)
    mq.process_preprocessor(claimed, key, raw)
    worker = ModelQ(redis_client=mq.redis_client._client, server_id="other-worker")
    worker.task(preprocessor=Pre)(getattr(mq, "compute"))
    ready = Task.from_dict(json.loads(mq.redis_client.lpop("ml_tasks")))
    assert ready.task_id == submitted.task_id
    worker.process_task(ready)
    assert submitted.get_result(mq.redis_client) == {"value": 8}
    assert called.call_count == 1
    assert mq.redis_client.llen(key) == 0


def test_abandoned_preprocessing_returns_to_preprocessing_queue(mq):
    class Pre(ProcessorMiddleware):
        def start(self):
            return None

    @mq.task(preprocessor=Pre)
    def compute():
        return get_preprocessor_result()

    task = compute()
    _, key, _ = _claim_preprocessor(mq)
    assert mq.drain_inflight(key) == 1
    assert mq.redis_client.llen("ml_tasks") == 0
    restored = json.loads(mq.redis_client.lpop(mq.PREPROCESSING_QUEUE))
    assert restored["task_id"] == task.task_id


def test_cancel_while_preprocessing_does_not_enqueue_task_body(mq):
    entered, release = threading.Event(), threading.Event()

    class Pre(ProcessorMiddleware):
        def start(self):
            entered.set()
            assert release.wait(5)
            return 1

    @mq.task(preprocessor=Pre)
    def compute():
        pytest.fail("cancelled task body must not run")

    task = compute()
    claimed, key, raw = _claim_preprocessor(mq)
    mq.redis_client.sadd("processing_tasks", task.task_id)
    mq._mark_task_inflight(task.task_id, time.time())
    thread = threading.Thread(target=mq.process_preprocessor, args=(claimed, key, raw))
    thread.start()
    try:
        assert entered.wait(5)
        assert mq.cancel_task(task.task_id)
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive()
    assert mq.get_task_status(task.task_id) == "cancelled"
    assert mq.redis_client.llen("ml_tasks") == 0
    assert not mq.redis_client.sismember("processing_tasks", task.task_id)
    assert task.task_id not in mq._inflight_tasks


def test_preprocessing_failure_and_non_json_results_do_not_reach_execution(mq):
    class Pre(ProcessorMiddleware):
        def start(self, value):
            if value:
                raise ValueError("pre failed")
            return object()

    @mq.task(preprocessor=Pre)
    def compute(value):
        pytest.fail("preprocessing has not succeeded")

    for value in (True, False):
        submitted = compute(value)
        claimed, key, raw = _claim_preprocessor(mq)
        with pytest.raises(TaskProcessingError):
            mq.process_preprocessor(claimed, key, raw)
        assert mq.get_task_status(submitted.task_id) == "failed"
        assert mq.redis_client.zscore("queued_requests", submitted.task_id) is None
        assert mq.redis_client.llen("ml_tasks") == 0
        mq.redis_client.lrem(key, 1, raw)


def test_retry_runs_a_fresh_preprocessing_stage(mq):
    calls = []

    class Pre(ProcessorMiddleware):
        def start(self):
            calls.append(1)
            return len(calls)

    @mq.task(preprocessor=Pre)
    def compute():
        if get_preprocessor_result() == 1:
            raise RetryTaskException("try again")
        return get_preprocessor_result()

    compute()
    claimed, key, raw = _claim_preprocessor(mq)
    mq.process_preprocessor(claimed, key, raw)
    mq.process_task(Task.from_dict(json.loads(mq.redis_client.lpop("ml_tasks"))))
    retry = json.loads(mq.redis_client.zrange("delayed_tasks", 0, -1)[0])
    assert retry["payload"][mq.PREPROCESSING_PENDING]
    assert mq.PREPROCESSOR_RESULT not in retry["payload"]
    mq.enqueue_task(retry, retry["payload"])
    claimed, key, raw = _claim_preprocessor(mq)
    mq.process_preprocessor(claimed, key, raw)
    done = Task.from_dict(json.loads(mq.redis_client.lpop("ml_tasks")))
    mq.process_task(done)
    assert done.result == 2
    assert len(calls) == 2


def test_cancel_removes_work_waiting_in_preprocessing_queue(mq):
    class Pre(ProcessorMiddleware):
        def start(self):
            pytest.fail("cancelled before preprocessing")

    @mq.task(preprocessor=Pre)
    def compute():
        pass

    task = compute()
    assert len(mq.get_all_queued_tasks()) == 1
    assert mq.cancel_task(task.task_id)
    assert mq.redis_client.llen(mq.PREPROCESSING_QUEUE) == 0
    assert mq.redis_client.zscore("queued_requests", task.task_id) is None


def test_preprocessing_timeout_never_publishes_execution_work(mq):
    release, exited = threading.Event(), threading.Event()

    class Pre(ProcessorMiddleware):
        def start(self):
            try:
                assert release.wait(5)
            finally:
                exited.set()
            return 1

    @mq.task(timeout=0.05, preprocessor=Pre)
    def compute():
        pytest.fail("timed-out preprocessing must not reach task body")

    task = compute()
    claimed, key, raw = _claim_preprocessor(mq)
    try:
        with pytest.raises(TaskProcessingError, match="timeout"):
            mq.process_preprocessor(claimed, key, raw)
    finally:
        release.set()
    assert exited.wait(5)
    assert mq.get_task_status(task.task_id) == "failed"
    assert mq.redis_client.llen("ml_tasks") == 0


def test_none_preprocessor_result_is_ready_and_not_run_twice(mq):
    called = Mock()

    class Pre(ProcessorMiddleware):
        def start(self):
            called()
            return None

    @mq.task(preprocessor=Pre)
    def compute():
        assert get_preprocessor_result() is None
        return 1

    compute()
    claimed, key, raw = _claim_preprocessor(mq)
    mq.process_preprocessor(claimed, key, raw)
    ready = Task.from_dict(json.loads(mq.redis_client.lpop("ml_tasks")))
    mq.process_task(ready)
    assert ready.result == 1
    assert called.call_count == 1
