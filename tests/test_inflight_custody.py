"""Tests for in-flight task custody.

Proven against production on 2026-08-15: with a worker's connection blackholed,
a task pushed to `ml_tasks` was popped by Redis, written into the dead socket,
and lost. It was not in the queue, not in `processing_tasks`, and no worker ever
logged receiving it — invisible to every recovery path.

BLMOVE closes that hole by making "take the task" and "record who took it" one
atomic step.
"""

import json
import threading
import time

import fakeredis
import pytest

from modelq import ModelQ


@pytest.fixture
def mq():
    return ModelQ(redis_client=fakeredis.FakeStrictRedis(), server_id="srv-a")


def _task(task_id="t1", name="noop"):
    return json.dumps(
        {"task_id": task_id, "task_name": name, "payload": {}, "status": "queued"}
    )


# ---------------------------------------------------------------------------
# custody
# ---------------------------------------------------------------------------

def test_a_task_in_transit_is_never_in_neither_list(mq):
    """The whole point: after the move the task is still somewhere findable.

    Under BLPOP this window is where the task ceased to exist.
    """
    inflight = mq._inflight_key(0)
    mq.redis_client.rpush("ml_tasks", _task())

    moved = mq.redis_client.blmove("ml_tasks", inflight, 1, "LEFT", "RIGHT")

    assert moved is not None
    assert mq.redis_client.llen("ml_tasks") == 0, "left the queue"
    assert mq.redis_client.llen(inflight) == 1, "but is held in custody, not lost"


def test_drain_returns_held_tasks_to_the_queue(mq):
    inflight = mq._inflight_key(0)
    mq.redis_client.rpush(inflight, _task("t1"), _task("t2"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, inflight)

    assert mq.drain_inflight(inflight) == 2
    assert mq.redis_client.llen("ml_tasks") == 2
    assert mq.redis_client.llen(inflight) == 0
    assert mq.redis_client.smembers(mq.INFLIGHT_REGISTRY) == set()


def test_drain_clears_stale_processing_marker_before_task_is_visible(mq):
    """A mid-task crash must not trip the next worker's duplicate guard."""
    inflight = mq._inflight_key(0)
    mq.redis_client.rpush(inflight, _task("crashed"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, inflight)
    mq.redis_client.sadd("processing_tasks", "crashed")

    assert mq.drain_inflight(inflight) == 1

    recovered = json.loads(mq.redis_client.lindex("ml_tasks", 0))
    assert recovered["task_id"] == "crashed"
    assert recovered["status"] == "queued"
    assert not mq.redis_client.sismember("processing_tasks", "crashed")
    assert mq.redis_client.zscore("queued_requests", "crashed") is not None
    assert mq.redis_client.ttl("task:crashed") > 0

    # This is the exact guard that previously discarded the recovered copy.
    assert mq.redis_client.sadd("processing_tasks", "crashed") == 1


def test_drain_keeps_malformed_entries_recoverable(mq):
    """Bad payloads still move atomically instead of blocking the whole list."""
    inflight = mq._inflight_key(0)
    mq.redis_client.rpush(inflight, "not-json")
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, inflight)

    assert mq.drain_inflight(inflight) == 1
    assert mq.redis_client.lpop("ml_tasks") == b"not-json"


def test_drain_is_bounded_and_terminates_on_empty(mq):
    """An unbounded drain loop spins forever the day the list refills."""
    assert mq.drain_inflight(mq._inflight_key(9)) == 0


# ---------------------------------------------------------------------------
# recovery: whose lists get drained
# ---------------------------------------------------------------------------

def test_dead_servers_inflight_tasks_are_recovered(mq):
    dead = f"{mq.INFLIGHT_PREFIX}:srv-ghost:0"
    mq.redis_client.rpush(dead, _task("orphan"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, dead)

    recovered = mq.recover_abandoned_inflight_tasks(active_server_ids={"srv-a"})

    assert recovered == 1
    assert mq.redis_client.llen("ml_tasks") == 1


def test_a_live_servers_inflight_tasks_are_left_alone(mq):
    """Draining a running worker's list hands its task to somebody else."""
    live = f"{mq.INFLIGHT_PREFIX}:srv-b:0"
    mq.redis_client.rpush(live, _task("in-progress"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, live)

    recovered = mq.recover_abandoned_inflight_tasks(active_server_ids={"srv-a", "srv-b"})

    assert recovered == 0
    assert mq.redis_client.llen(live) == 1
    assert mq.redis_client.llen("ml_tasks") == 0


def test_own_list_is_skipped_during_the_periodic_sweep(mq):
    """Our own workers are mid-task; the sweep must not touch them."""
    mine = mq._inflight_key(0)
    mq.redis_client.rpush(mine, _task("mine"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, mine)

    assert mq.recover_abandoned_inflight_tasks(active_server_ids={"srv-a"}) == 0
    assert mq.redis_client.llen(mine) == 1


def test_own_list_is_drained_at_startup(mq):
    """At startup our own lists are debris from a previous run of this id."""
    mine = mq._inflight_key(0)
    mq.redis_client.rpush(mine, _task("leftover"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, mine)

    recovered = mq.recover_abandoned_inflight_tasks(
        active_server_ids={"srv-a"}, include_self=True
    )

    assert recovered == 1
    assert mq.redis_client.llen("ml_tasks") == 1


def test_malformed_registry_entry_does_not_abort_recovery(mq):
    """One bad key must not strand every other server's tasks."""
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, "garbage")
    dead = f"{mq.INFLIGHT_PREFIX}:srv-ghost:0"
    mq.redis_client.rpush(dead, _task("orphan"))
    mq.redis_client.sadd(mq.INFLIGHT_REGISTRY, dead)

    assert mq.recover_abandoned_inflight_tasks(active_server_ids={"srv-a"}) == 1


# ---------------------------------------------------------------------------
# end-to-end through the worker loop
# ---------------------------------------------------------------------------

def test_worker_registers_its_inflight_list_before_taking_work(mq):
    """A task must never be held in a list recovery does not know about."""
    mq.start_workers(no_of_workers=1)
    deadline = time.time() + 5
    while time.time() < deadline:
        if mq.redis_client.smembers(mq.INFLIGHT_REGISTRY):
            break
        time.sleep(0.05)

    members = {
        m.decode() if isinstance(m, bytes) else m
        for m in mq.redis_client.smembers(mq.INFLIGHT_REGISTRY)
    }
    assert mq._inflight_key(0) in members


def test_worker_holds_custody_while_running_then_releases_it(mq):
    """The two halves of custody, observed on a real in-flight task.

    Holding it is what BLPOP cannot do — under BLPOP the task is in no list at
    all while it runs. Releasing it is what stops the list growing forever.
    """
    inflight = mq._inflight_key(0)
    running = threading.Event()
    may_finish = threading.Event()

    @mq.task()
    def slow_task():
        running.set()
        may_finish.wait(timeout=10)
        return "done"

    mq.start_workers(no_of_workers=1)
    slow_task()

    assert running.wait(timeout=10), "worker never started the task"

    # Mid-flight: the task must be recorded as held by this worker.
    assert mq.redis_client.llen(inflight) == 1, (
        "task is in flight but held in no list — it would be unrecoverable"
    )

    may_finish.set()

    deadline = time.time() + 10
    while time.time() < deadline and mq.redis_client.llen(inflight) != 0:
        time.sleep(0.05)
    assert mq.redis_client.llen(inflight) == 0, "custody was never released"
