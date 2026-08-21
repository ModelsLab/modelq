"""
Tests for claiming a task on Redis versions without BLMOVE.

BLMOVE needs Redis >= 6.2. Every GPU worker container in production was still on
Redis 6.0.16, where the command does not exist. Upgrading such a worker to a
ModelQ that calls BLMOVE unconditionally produced:

    ERROR - Worker 0 crashed with error: unknown command `BLMOVE`,
            with args beginning with: `ml_tasks`, `inflight:...`, `LEFT`, ...

roughly thirty times a second, forever. The worker never registered, the queue
never drained, and nothing about the process looked unhealthy from the outside --
`ps` showed it running and burning CPU. Requests simply queued and were answered
with an ever-growing ETA.

The fallback must keep both properties the BLMOVE claim was introduced for:
atomicity (the task is never in neither list) and FIFO order. BRPOPLPUSH, the
obvious Redis 6.0 substitute, satisfies the first and breaks the second -- it
pops the tail, which turns an rpush-fed queue LIFO. Hence the Lua LPOP+RPUSH.
"""

import json

import fakeredis
import pytest
import redis as redis_lib

from modelq import ModelQ


class RedisWithoutBlmove(fakeredis.FakeStrictRedis):
    """A Redis 6.0-era server: everything works except BLMOVE."""

    def blmove(self, *args, **kwargs):
        raise redis_lib.exceptions.ResponseError(
            "unknown command `BLMOVE`, with args beginning with: `ml_tasks`, "
            "`inflight:abc:0`, `LEFT`, `RIGHT`, `15`, "
        )


@pytest.fixture
def old_redis():
    return RedisWithoutBlmove()


@pytest.fixture
def new_redis():
    return fakeredis.FakeStrictRedis()


def _queue(mq, *task_ids):
    for task_id in task_ids:
        mq.redis_client.rpush(
            "ml_tasks",
            json.dumps({"task_id": task_id, "task_name": "generate", "payload": {}}),
        )


def _ids(raw_items):
    out = []
    for raw in raw_items:
        blob = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
        out.append(json.loads(blob)["task_id"])
    return out


def test_claims_a_task_when_redis_has_no_blmove(old_redis):
    mq = ModelQ(redis_client=old_redis)
    mq.BLPOP_TIMEOUT = 1
    _queue(mq, "task-1")

    claimed = mq._claim_task("inflight:worker:0")

    assert claimed is not None, "worker cannot pick up any work at all"
    assert _ids([claimed]) == ["task-1"]


def test_fallback_keeps_the_task_in_exactly_one_list(old_redis):
    """
    Atomicity is the whole reason the claim stopped being a plain BLPOP: the task
    must never be absent from both the queue and the in-flight list.
    """
    mq = ModelQ(redis_client=old_redis)
    mq.BLPOP_TIMEOUT = 1
    _queue(mq, "task-1")

    mq._claim_task("inflight:worker:0")

    assert mq.redis_client.llen("ml_tasks") == 0
    assert _ids(mq.redis_client.lrange("inflight:worker:0", 0, -1)) == ["task-1"]


def test_fallback_preserves_fifo_order(old_redis):
    """
    The regression BRPOPLPUSH would have introduced. Producers rpush, so claims
    must come off the head -- otherwise the newest request jumps the queue and
    the oldest starves.
    """
    mq = ModelQ(redis_client=old_redis)
    mq.BLPOP_TIMEOUT = 1
    _queue(mq, "first", "second", "third")

    claimed = [mq._claim_task("inflight:worker:0") for _ in range(3)]

    assert _ids(claimed) == ["first", "second", "third"]


def test_fallback_returns_none_on_an_empty_queue_without_hanging(old_redis):
    mq = ModelQ(redis_client=old_redis)
    mq.BLPOP_TIMEOUT = 0.3
    mq.CLAIM_POLL_INTERVAL = 0.05

    assert mq._claim_task("inflight:worker:0") is None


def test_modern_redis_still_uses_blmove(new_redis):
    """The control case: a capable server must not be downgraded to polling."""
    mq = ModelQ(redis_client=new_redis)
    mq.BLPOP_TIMEOUT = 1
    _queue(mq, "task-1")

    calls = []
    real_blmove = new_redis.blmove

    def spy(*args, **kwargs):
        calls.append(args)
        return real_blmove(*args, **kwargs)

    new_redis.blmove = spy
    claimed = mq._claim_task("inflight:worker:0")

    assert calls, "BLMOVE should still be used where it exists"
    assert _ids([claimed]) == ["task-1"]


def test_support_is_probed_once_not_per_claim(old_redis):
    """
    An unsupported server must not cost a failed round trip on every claim -- the
    crash loop was thirty exceptions a second.
    """
    probes = []
    original = old_redis.blmove

    def counting_blmove(*args, **kwargs):
        probes.append(args)
        return original(*args, **kwargs)

    old_redis.blmove = counting_blmove

    mq = ModelQ(redis_client=old_redis)
    mq.BLPOP_TIMEOUT = 1
    _queue(mq, "a", "b", "c")

    for _ in range(3):
        mq._claim_task("inflight:worker:0")

    assert len(probes) == 1


def test_an_unrelated_redis_error_does_not_disable_blmove(new_redis):
    """
    Only "unknown command" means the server lacks BLMOVE. A transient server-side
    error must not permanently drop the worker into polling mode.
    """
    mq = ModelQ(redis_client=new_redis)

    def transient(*args, **kwargs):
        raise redis_lib.exceptions.ResponseError("LOADING Redis is loading the dataset")

    new_redis.blmove = transient

    assert mq._blmove_supported() is True
