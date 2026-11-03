"""Processors execute per worker attempt, outside the task body."""

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import fakeredis
import pytest
from pydantic import BaseModel

from modelq import ModelQ
from modelq.app.tasks import Task
from modelq.exceptions import RetryTaskException, TaskProcessingError
from modelq.middleware import (
    ProcessorMiddleware,
    Processor_middlware,
    get_preprocessor_result,
    send_postprocessor,
)


@pytest.fixture
def mq():
    queue = ModelQ(redis_client=fakeredis.FakeStrictRedis())
    queue.log_task_error_to_file = Mock()
    return queue


def test_precedes_task_and_background_post_starts_after_persistence(mq):
    started, release = threading.Event(), threading.Event()
    futures = []

    class Pre(Processor_middlware):
        async def start(self, param):
            param["a"] += 10
            return param["a"]

    class Post(ProcessorMiddleware):
        async def start(self, param):
            assert mq.get_task_status(task.task_id) == "completed"
            started.set()
            assert release.wait(5)
            return param["value"]

    @mq.task(preprocessor=Pre, postprocessor=Post)
    def add(param):
        assert get_preprocessor_result() == 12
        assert param["a"] == 2
        data = {"value": 42}
        futures.append(send_postprocessor(data))
        data["value"] = -1
        assert not started.is_set()
        return param["a"] + param["b"]

    task = add({"a": 2, "b": 3})
    try:
        mq.process_task(task)
        assert task.result == 5
        assert started.wait(5)
        assert not futures[0].done()
    finally:
        release.set()
    assert futures[0].result(timeout=5) == 42
    with pytest.raises(RuntimeError, match="inside a running task"):
        get_preprocessor_result()


def test_concurrent_attempt_contexts_are_isolated(mq):
    barrier = threading.Barrier(2)

    class Pre(ProcessorMiddleware):
        def start(self, value, *, offset):
            return value + offset

    @mq.task(preprocessor=Pre, timeout=5)
    def compute(value, *, offset):
        barrier.wait(timeout=5)
        return get_preprocessor_result()

    tasks = [compute(1, offset=10), compute(2, offset=20)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(mq.process_task, tasks))
    assert [task.result for task in tasks] == [11, 22]


def test_preprocessor_failure_prevents_task_body(mq):
    called = Mock()

    class Pre(ProcessorMiddleware):
        def start(self):
            raise ValueError("preprocessing failed")

    @mq.task(preprocessor=Pre)
    def compute():
        called()

    task = compute()
    with pytest.raises(TaskProcessingError, match="preprocessing failed"):
        mq.process_task(task)
    called.assert_not_called()
    assert task.status == "failed"


@pytest.mark.parametrize("retry", [False, True])
def test_unsuccessful_attempt_discards_postprocessing(mq, retry):
    called, futures = Mock(), []

    class Post(ProcessorMiddleware):
        def start(self, data):
            called(data)

    @mq.task(postprocessor=Post)
    def compute():
        futures.append(send_postprocessor("data"))
        if retry:
            raise RetryTaskException("retry")
        raise ValueError("failed")

    task = compute()
    if retry:
        mq.process_task(task)
    else:
        with pytest.raises(TaskProcessingError):
            mq.process_task(task)
    assert futures[0].cancelled()
    called.assert_not_called()


def test_postprocessor_failure_is_observable_without_failing_task(mq):
    futures = []

    class Post(ProcessorMiddleware):
        def start(self):
            raise ValueError("post failed")

    @mq.task(postprocessor=Post)
    def compute():
        futures.append(send_postprocessor())
        return 7

    task = compute()
    mq.process_task(task)
    with pytest.raises(ValueError, match="post failed"):
        futures[0].result(timeout=5)
    assert task.status == "completed"
    assert task.result == 7


def test_streaming_keeps_context_until_generator_finishes(mq):
    futures = []

    class Pre(ProcessorMiddleware):
        def start(self, value):
            return value * 2

    class Post(ProcessorMiddleware):
        def start(self, value):
            return value + 1

    @mq.task(stream=True, preprocessor=Pre, postprocessor=Post)
    def stream(value):
        yield get_preprocessor_result()
        futures.append(send_postprocessor(value))
        yield get_preprocessor_result() + 1

    task = stream(3)
    mq.process_task(task)
    assert task.status == "completed"
    assert futures[0].result(timeout=5) == 4
    assert mq.redis_client.xlen(f"task_stream:{task.task_id}") == 2


def test_schema_processor_receives_validated_model(mq):
    class Input(BaseModel):
        value: int

    class Pre(ProcessorMiddleware):
        def start(self, param):
            assert isinstance(param, Input)
            return param.value * 2

    @mq.task(schema=Input, preprocessor=Pre)
    def compute(param):
        return get_preprocessor_result()

    task = compute(value="3")
    mq.process_task(Task.from_dict(task.to_dict()))
    assert task.get_result(mq.redis_client) == 6


def test_timeout_includes_preprocessing_and_prevents_late_task_start(mq):
    entered, release, exited = threading.Event(), threading.Event(), threading.Event()
    called = Mock()

    class Pre(ProcessorMiddleware):
        def start(self):
            entered.set()
            assert release.wait(5)

    @mq.task(timeout=0.05, preprocessor=Pre)
    def compute():
        called()

    original = mq._run_with_timeout

    def run(function, timeout, *args, **kwargs):
        def wrapped(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            finally:
                exited.set()
        return original(wrapped, timeout, *args, **kwargs)

    mq._run_with_timeout = run
    try:
        with pytest.raises(TaskProcessingError, match="timeout"):
            mq.process_task(compute())
        assert entered.is_set()
    finally:
        release.set()
    assert exited.wait(5)
    called.assert_not_called()


def test_processor_configuration_and_helper_errors(mq):
    with pytest.raises(TypeError, match="subclasses"):
        mq.task(preprocessor=object)
    with pytest.raises(RuntimeError, match="inside a running task"):
        send_postprocessor({})

    @mq.task()
    def compute():
        assert get_preprocessor_result() is None
        send_postprocessor({})

    with pytest.raises(TaskProcessingError, match="No postprocessor"):
        mq.process_task(compute())


def test_timed_out_task_discards_staged_postprocessing(mq):
    staged, release, exited = threading.Event(), threading.Event(), threading.Event()
    futures, called = [], Mock()

    class Post(ProcessorMiddleware):
        def start(self):
            called()

    @mq.task(timeout=0.05, postprocessor=Post)
    def compute():
        try:
            futures.append(send_postprocessor())
            staged.set()
            assert release.wait(5)
            return 1
        finally:
            exited.set()

    task = compute()
    try:
        with pytest.raises(TaskProcessingError, match="timeout"):
            mq.process_task(task)
        assert staged.is_set()
        assert futures[0].cancelled()
    finally:
        release.set()
    assert exited.wait(5)
    assert task.status == "failed"
    called.assert_not_called()
