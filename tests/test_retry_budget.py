"""Regression tests for the RetryTaskException retry budget.

A RetryTaskException used to be re-queued unconditionally, with no attempt
counter. Any task whose failure was permanent -- an expired or deleted source
URL, for example -- looped forever, re-entering the queue every delay_seconds
and consuming worker capacity that could never produce a result. These tests
pin the bound.
"""

import fakeredis
import pytest

from modelq import ModelQ
from modelq.app.tasks.base import Task
from modelq.exceptions import RetryTaskException


@pytest.fixture
def mock_redis():
    return fakeredis.FakeStrictRedis()


def _register_doomed(mq):
    """Register a task that always asks to be retried."""
    def doomed(*args, **kwargs):
        raise RetryTaskException("source image is gone for good")

    # process_task resolves the callable via getattr(self, task.task_name)
    mq.allowed_tasks.add("doomed")
    setattr(mq, "doomed", doomed)


def _queued_delayed(mq):
    """Number of tasks sitting in the delayed set."""
    return mq.redis_client.zcard("delayed_tasks")


def test_permanently_failing_task_stops_retrying(mock_redis):
    mq = ModelQ(redis_client=mock_redis, retry_task_max_attempts=2)
    _register_doomed(mq)

    task = Task(task_name="doomed", payload={"data": {"args": [], "kwargs": {}}, "init_image": "https://gone.example/x.png"})

    # Attempt 1 -> re-queued, counter starts.
    mq.process_task(task)
    assert task.original_payload["_retry_attempts"] == 1
    assert task.status != "failed"

    # Attempt 2 -> re-queued, counter advances.
    mq.process_task(task)
    assert task.original_payload["_retry_attempts"] == 2
    assert task.status != "failed"

    # Attempt 3 exceeds the budget -> the task is failed, not re-queued.
    before = _queued_delayed(mq)
    mq.process_task(task)
    assert task.status == "failed"
    assert "Retry limit reached" in str(task.result)
    assert _queued_delayed(mq) == before, "must not re-queue once the budget is spent"


def test_reserved_counter_does_not_consume_the_callers_retries_budget(mock_redis):
    mq = ModelQ(redis_client=mock_redis, retry_task_max_attempts=5)
    _register_doomed(mq)

    task = Task(task_name="doomed", payload={"data": {"args": [], "kwargs": {}}, "retries": 4})
    mq.process_task(task)

    # "retries" belongs to the TaskProcessingError path and must be untouched.
    assert task.original_payload["retries"] == 4
    assert task.original_payload["_retry_attempts"] == 1


def test_zero_budget_fails_on_first_retry_request(mock_redis):
    mq = ModelQ(redis_client=mock_redis, retry_task_max_attempts=0)
    _register_doomed(mq)

    task = Task(task_name="doomed", payload={"data": {"args": [], "kwargs": {}}})
    mq.process_task(task)

    assert task.status == "failed"
    assert "Retry limit reached" in str(task.result)


def test_default_budget_is_finite(mock_redis):
    """The old behaviour was unbounded; the default must not be."""
    mq = ModelQ(redis_client=mock_redis)
    assert isinstance(mq.retry_task_max_attempts, int)
    assert 0 < mq.retry_task_max_attempts < 100
