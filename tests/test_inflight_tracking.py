"""
Tests for in-flight task liveness.

Two production failures motivate this file:

  1. `started_at` was written into the task dict at pickup but never onto the
     Task object, so `_store_final_task_state()` -- which serialises from the
     object -- overwrote the real pickup time with null on every completion.
     Nothing downstream could separate queue wait from run time, and the
     requeue sweep (which keys off `started_at`) skipped finished strays
     forever, leaving `processing_tasks` to grow until the task keys expired.

  2. The sweep decided "stuck" from age alone. A 500s video generation crosses
     a 180s threshold while running perfectly well, so it was pushed back onto
     `ml_tasks` and rendered a second time on another GPU.
"""

import json
import time

import fakeredis
import pytest

from modelq import ModelQ


@pytest.fixture
def mock_redis():
    return fakeredis.FakeStrictRedis()


@pytest.fixture
def mq(mock_redis):
    return ModelQ(redis_client=mock_redis)


def _blob(raw):
    return json.loads(raw.decode() if isinstance(raw, (bytes, bytearray)) else raw)


def _place_in_processing(mq, task_id, *, status, started_at, heartbeat=None):
    """Put a task into `processing_tasks` exactly as a worker pickup would."""
    mq.redis_client.sadd("processing_tasks", task_id)
    mq.redis_client.set(
        f"task:{task_id}",
        json.dumps(
            {
                "task_id": task_id,
                "task_name": "render",
                "payload": {"data": {"args": []}},
                "status": status,
                "result": None,
                "created_at": started_at,
                "queued_at": started_at,
                "started_at": started_at,
                "finished_at": None,
                "stream": False,
            }
        ),
    )
    if heartbeat is not None:
        mq.redis_client.hset(ModelQ.INFLIGHT_HEARTBEAT_KEY, task_id, str(heartbeat))


# ---------------------------------------------------------------------------
# the sweep
# ---------------------------------------------------------------------------


def test_long_running_task_with_fresh_heartbeat_is_not_requeued(mq):
    """A 10-minute job whose worker is alive must be left alone."""
    now = time.time()
    _place_in_processing(
        mq, "video-1", status="processing", started_at=now - 600, heartbeat=now - 5
    )

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert mq.redis_client.llen("ml_tasks") == 0, "healthy long job was re-queued"
    assert mq.redis_client.sismember("processing_tasks", "video-1")
    assert _blob(mq.redis_client.get("task:video-1"))["status"] == "processing"


def test_task_is_requeued_once_its_heartbeat_goes_stale(mq):
    """The worker died holding it; nobody is producing this output."""
    now = time.time()
    _place_in_processing(
        mq, "video-2", status="processing", started_at=now - 600, heartbeat=now - 600
    )

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert mq.redis_client.llen("ml_tasks") == 1
    assert _blob(mq.redis_client.lindex("ml_tasks", 0))["task_id"] == "video-2"
    assert not mq.redis_client.sismember("processing_tasks", "video-2")
    assert mq.redis_client.hget(ModelQ.INFLIGHT_HEARTBEAT_KEY, "video-2") is None


def test_stale_task_moves_out_of_custody_instead_of_leaving_a_duplicate(mq):
    """Heartbeat recovery and BLMOVE custody must produce one queue copy."""
    now = time.time()
    task_id = "custodied-stale"
    _place_in_processing(
        mq,
        task_id,
        status="processing",
        started_at=now - 600,
        heartbeat=now - 600,
    )
    inflight = mq._inflight_key(0)
    mq.redis_client.rpush(inflight, mq.redis_client.get(f"task:{task_id}"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, inflight)

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert mq.redis_client.llen("ml_tasks") == 1
    assert mq.redis_client.llen(inflight) == 0
    assert not mq.redis_client.sismember("processing_tasks", task_id)
    assert mq.redis_client.sismember(mq.INFLIGHT_REGISTRY, inflight)
    assert _blob(mq.redis_client.lindex("ml_tasks", 0))["task_id"] == task_id


def test_heartbeat_freshness_is_measured_against_the_threshold(mq):
    """A job younger than the threshold is safe even with no heartbeat yet."""
    now = time.time()
    _place_in_processing(mq, "fresh", status="processing", started_at=now - 30)

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert mq.redis_client.llen("ml_tasks") == 0
    assert mq.redis_client.sismember("processing_tasks", "fresh")


def test_task_without_heartbeat_falls_back_to_the_age_rule(mq):
    """
    Mixed-version fleets: a worker on an older build publishes no heartbeat, so
    the sweep must behave exactly as it did before rather than never acting.
    """
    now = time.time()
    _place_in_processing(mq, "legacy", status="processing", started_at=now - 600)

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert mq.redis_client.llen("ml_tasks") == 1
    assert not mq.redis_client.sismember("processing_tasks", "legacy")


def test_finished_stray_is_drained_not_rerun(mq):
    """
    The regression guard for fix (1). Once `started_at` survives completion, a
    finished task left behind in `processing_tasks` looks exactly like an old
    stuck one. Re-queueing it would re-run a generation that already produced
    its output.
    """
    now = time.time()
    _place_in_processing(mq, "done-1", status="completed", started_at=now - 600)

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert mq.redis_client.llen("ml_tasks") == 0, "completed task was re-run"
    assert not mq.redis_client.sismember("processing_tasks", "done-1")


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
def test_every_terminal_status_is_drained(mq, status):
    now = time.time()
    _place_in_processing(mq, f"t-{status}", status=status, started_at=now - 600)

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert mq.redis_client.llen("ml_tasks") == 0
    assert not mq.redis_client.sismember("processing_tasks", f"t-{status}")


def test_missing_task_key_is_dropped_from_the_set(mq):
    mq.redis_client.sadd("processing_tasks", "ghost")

    mq.requeue_stuck_processing_tasks(threshold=180)

    assert not mq.redis_client.sismember("processing_tasks", "ghost")
    assert mq.redis_client.llen("ml_tasks") == 0


def test_live_task_is_not_read_back_from_redis(mq):
    """
    The sweep runs once a minute on every worker and used to GET every in-flight
    payload -- hundreds of KB each on image queues -- only to decide to do
    nothing. A fresh heartbeat should short-circuit before the fetch.
    """
    now = time.time()
    _place_in_processing(
        mq, "busy", status="processing", started_at=now - 600, heartbeat=now - 5
    )

    reads = []
    real_get = mq.redis_client.get

    def counting_get(key, *args, **kwargs):
        reads.append(key)
        return real_get(key, *args, **kwargs)

    mq.redis_client.get = counting_get
    try:
        mq.requeue_stuck_processing_tasks(threshold=180)
    finally:
        mq.redis_client.get = real_get

    assert reads == [], f"payload was fetched anyway: {reads}"


# ---------------------------------------------------------------------------
# heartbeat bookkeeping
# ---------------------------------------------------------------------------


def test_marking_a_task_inflight_publishes_a_heartbeat(mq):
    mq._mark_task_inflight("abc", 1234.5)

    assert mq._inflight_tasks == {"abc": 1234.5}
    assert float(mq.redis_client.hget(ModelQ.INFLIGHT_HEARTBEAT_KEY, "abc")) == 1234.5


def test_clearing_a_task_removes_the_heartbeat(mq):
    mq._mark_task_inflight("abc", 1234.5)
    mq._clear_task_inflight("abc")

    assert mq._inflight_tasks == {}
    assert mq.redis_client.hget(ModelQ.INFLIGHT_HEARTBEAT_KEY, "abc") is None


def test_publishing_advances_every_inflight_heartbeat(mq):
    mq._mark_task_inflight("a", time.time() - 500)
    mq._mark_task_inflight("b", time.time() - 500)

    mq._publish_inflight_heartbeats()

    beats = mq._inflight_heartbeats()
    now = time.time()
    assert set(beats) == {"a", "b"}
    assert all(now - v < 5 for v in beats.values())


def test_publishing_with_nothing_inflight_touches_nothing(mq):
    mq._publish_inflight_heartbeats()
    assert mq.redis_client.hgetall(ModelQ.INFLIGHT_HEARTBEAT_KEY) == {}


def test_unreadable_heartbeat_values_are_ignored(mq):
    mq.redis_client.hset(ModelQ.INFLIGHT_HEARTBEAT_KEY, "junk", "not-a-float")
    mq.redis_client.hset(ModelQ.INFLIGHT_HEARTBEAT_KEY, "good", "100.0")

    assert mq._inflight_heartbeats() == {"good": 100.0}


# ---------------------------------------------------------------------------
# end-to-end: the started_at fix, through a real worker
# ---------------------------------------------------------------------------


def _wait_for(predicate, timeout=10.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_started_at_survives_a_real_completion(mock_redis):
    """
    The headline regression. Run one task through an actual worker and assert
    the finished record still carries the pickup time.

    Before the fix this came back as null on every completed task, because
    `_store_final_task_state()` re-serialised a Task object whose `started_at`
    had only ever been written to a separate dict.
    """
    mq = ModelQ(redis_client=mock_redis)

    @mq.task(timeout=30)
    def render():
        time.sleep(0.25)
        return {"ok": True}

    render(_task_id="e2e-1")
    mq.start_workers(no_of_workers=1)

    assert _wait_for(
        lambda: _blob(mock_redis.get("task:e2e-1")).get("status") == "completed"
    ), "task never completed"

    record = _blob(mock_redis.get("task:e2e-1"))
    assert record["started_at"] is not None, "started_at was erased on completion"
    assert record["finished_at"] is not None
    assert record["finished_at"] >= record["started_at"]
    assert record["started_at"] >= record["queued_at"]

    # run time is now derivable, which is the whole point
    assert 0.2 <= record["finished_at"] - record["started_at"] < 5.0

    # and the worker cleaned up after itself
    assert _wait_for(lambda: not mock_redis.sismember("processing_tasks", "e2e-1"))
    assert mock_redis.hget(ModelQ.INFLIGHT_HEARTBEAT_KEY, "e2e-1") is None


def test_a_task_is_marked_inflight_while_it_runs(mock_redis):
    """The heartbeat must exist during the run, not only after it."""
    mq = ModelQ(redis_client=mock_redis)
    seen = {}

    @mq.task(timeout=30)
    def slow():
        seen["inflight"] = dict(mq._inflight_tasks)
        seen["heartbeat"] = mock_redis.hget(ModelQ.INFLIGHT_HEARTBEAT_KEY, "e2e-2")
        seen["processing"] = mock_redis.sismember("processing_tasks", "e2e-2")
        return {"ok": True}

    slow(_task_id="e2e-2")
    mq.start_workers(no_of_workers=1)

    assert _wait_for(lambda: "inflight" in seen), "task never ran"

    assert "e2e-2" in seen["inflight"], "task was not registered in-flight"
    assert seen["heartbeat"] is not None, "no heartbeat published while running"
    assert seen["processing"]


def test_task_routed_to_another_worker_does_not_leak_a_heartbeat(mock_redis):
    """Requeued work is not live here and must never suppress recovery."""
    import threading

    mq = ModelQ(redis_client=mock_redis, server_id="routing-worker")
    now = time.time()
    task_id = "wrong-worker-task"
    task = {
        "task_id": task_id,
        "task_name": "not-allowed-here",
        "payload": {},
        "status": "queued",
        "result": None,
        "created_at": now,
        "queued_at": now,
        "started_at": None,
        "finished_at": None,
        "stream": False,
    }
    mock_redis.lpush("ml_tasks", json.dumps(task))

    requeued = threading.Event()
    real_rpush = mock_redis.rpush

    def stop_after_requeue(name, *values):
        result = real_rpush(name, *values)
        if name == "ml_tasks":
            mq.worker_healthy = False
            requeued.set()
        return result

    mock_redis.rpush = stop_after_requeue
    try:
        mq.start_workers(no_of_workers=1)
        assert requeued.wait(timeout=5), "task was not routed back to the queue"
    finally:
        mock_redis.rpush = real_rpush

    assert task_id not in mq._inflight_tasks
    assert mock_redis.hget(ModelQ.INFLIGHT_HEARTBEAT_KEY, task_id) is None
    assert not mock_redis.sismember("processing_tasks", task_id)
    assert mock_redis.llen(mq._inflight_key(0)) == 0
    assert mock_redis.llen("ml_tasks") == 1
