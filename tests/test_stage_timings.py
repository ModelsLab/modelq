"""
Tests for the queue/run split a producer sees after blocking on a result.

Motivated by a production report where a generation API answered in ~37s while
the only timer it exposed read 5.55s. That timer started after the task was
enqueued, so everything before it -- and the queue wait itself -- was invisible,
and there was no way to tell a slow queue apart from a slow run.

Two gaps made the split unreportable:

  1. The task decorator wrote `created_at`/`queued_at` into the dict it pushed to
     Redis but never onto the Task object it handed back, so the producer's
     `task.queued_at` stayed None.

  2. `get_result()` copied only `result` and `status` off the terminal blob and
     dropped `started_at`/`finished_at`, so even a worker that recorded them
     could not surface them to the caller that was waiting.
"""

import json
import time

import fakeredis
import pytest

from modelq import ModelQ
from modelq.app.tasks.base import Task


@pytest.fixture
def mock_redis():
    return fakeredis.FakeStrictRedis()


@pytest.fixture
def mq(mock_redis):
    return ModelQ(redis_client=mock_redis)


def _complete(mq, task_id, *, queued_at, started_at, finished_at, result="done"):
    """Write the terminal blob exactly as `_store_final_task_state()` would."""
    mq.redis_client.set(
        f"task_result:{task_id}",
        json.dumps(
            {
                "task_id": task_id,
                "task_name": "generate",
                "payload": {},
                "status": "completed",
                "result": result,
                "created_at": queued_at,
                "queued_at": queued_at,
                "started_at": started_at,
                "finished_at": finished_at,
            }
        ),
    )


def test_enqueue_stamps_queued_at_on_the_returned_task(mq):
    @mq.task()
    def generate(payload=None):
        return "ok"

    before = time.time()
    task = generate({"prompt": "a cat"})
    after = time.time()

    assert task.queued_at is not None, "producer cannot measure queue wait"
    assert before <= task.queued_at <= after
    assert task.created_at is not None


def test_get_result_reports_queue_and_run_split(mq):
    @mq.task()
    def generate(payload=None):
        return "ok"

    task = generate({"prompt": "a cat"})

    # 12s waiting behind other work, then a 5.5s run.
    queued_at = task.queued_at
    _complete(
        mq,
        task.task_id,
        queued_at=queued_at,
        started_at=queued_at + 12.0,
        finished_at=queued_at + 17.5,
    )

    assert task.get_result(mq.redis_client, timeout=1) == "done"

    timings = task.stage_timings()
    assert timings["queue_time"] == pytest.approx(12.0, abs=0.01)
    assert timings["run_time"] == pytest.approx(5.5, abs=0.01)
    assert timings["total_time"] == pytest.approx(17.5, abs=0.01)


def test_stage_timings_omits_stages_it_cannot_measure(mq):
    """
    A stage with a missing endpoint must be absent, not reported as 0.0 -- a
    fabricated zero reads as "instant" and would hide the very stall this split
    exists to expose.
    """
    task = Task(task_name="generate", payload={})
    task.queued_at = 1000.0
    task.started_at = None
    task.finished_at = None

    assert task.stage_timings() == {}

    task.started_at = 1003.0
    timings = task.stage_timings()
    assert timings == {"queue_time": 3.0}


def test_timestamps_survive_a_failed_task(mq):
    """
    A failure is exactly when the split matters most, so the timestamps must be
    absorbed before get_result() raises.
    """

    @mq.task()
    def generate(payload=None):
        return "ok"

    task = generate({"prompt": "a cat"})
    queued_at = task.queued_at

    mq.redis_client.set(
        f"task_result:{task.task_id}",
        json.dumps(
            {
                "task_id": task.task_id,
                "task_name": "generate",
                "payload": {},
                "status": "failed",
                "result": "boom",
                "queued_at": queued_at,
                "started_at": queued_at + 2.0,
                "finished_at": queued_at + 9.0,
            }
        ),
    )

    with pytest.raises(Exception):
        task.get_result(mq.redis_client, timeout=1)

    timings = task.stage_timings()
    assert timings["queue_time"] == pytest.approx(2.0, abs=0.01)
    assert timings["run_time"] == pytest.approx(7.0, abs=0.01)


def test_older_worker_blob_without_timestamps_does_not_clobber(mq):
    """
    A worker running an older build omits the run timestamps. The queued_at the
    producer already holds must survive, rather than being reset to None.
    """

    @mq.task()
    def generate(payload=None):
        return "ok"

    task = generate({"prompt": "a cat"})
    queued_at = task.queued_at

    mq.redis_client.set(
        f"task_result:{task.task_id}",
        json.dumps(
            {
                "task_id": task.task_id,
                "task_name": "generate",
                "payload": {},
                "status": "completed",
                "result": "done",
            }
        ),
    )

    assert task.get_result(mq.redis_client, timeout=1) == "done"
    assert task.queued_at == queued_at
    assert task.stage_timings() == {}
