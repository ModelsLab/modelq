"""Tests for the reconnect delay in _RedisWithRetry.

Recovery from a dead Redis connection costs socket_timeout (to detect) plus
RETRY_DELAY (to wait), so RETRY_DELAY is the tail of every stall. It also has
to spread a synchronised herd: on 2026-08-15 every worker on a node logged the
same connection failure in the same second, because one upstream path change
broke all of them at once.
"""

from unittest.mock import patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from modelq.app.redis_retry import _RedisWithRetry


class _Flaky:
    """Fails `fail_times` then succeeds, recording each call."""

    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.calls = 0

    def ping(self):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RedisConnectionError("connection reset")
        return True


def test_retry_delay_is_short_enough_to_bound_a_stall():
    assert _RedisWithRetry.RETRY_DELAY <= 15, (
        "RETRY_DELAY is the tail of every reconnect; keep it small"
    )


def test_jittered_delay_stays_within_bounds():
    """Never zero (busy-loop) and never far above the nominal delay."""
    lo = _RedisWithRetry.RETRY_DELAY * (1 - _RedisWithRetry.RETRY_JITTER)
    hi = _RedisWithRetry.RETRY_DELAY * (1 + _RedisWithRetry.RETRY_JITTER)
    seen = {round(_RedisWithRetry._next_delay(), 4) for _ in range(200)}
    for d in seen:
        assert lo <= d <= hi, f"{d} outside [{lo}, {hi}]"
    assert min(seen) > 0, "a zero delay would busy-loop against a down Redis"


def test_jitter_actually_varies_the_delay():
    """Without this, a synchronised fleet retries in lockstep."""
    seen = {round(_RedisWithRetry._next_delay(), 6) for _ in range(50)}
    assert len(seen) > 1, "delay is constant — jitter is not being applied"


def test_jitter_can_be_disabled_for_determinism():
    with patch.object(_RedisWithRetry, "RETRY_JITTER", 0):
        assert _RedisWithRetry._next_delay() == _RedisWithRetry.RETRY_DELAY


def test_retry_loop_recovers_and_sleeps_the_jittered_delay():
    """The point of the whole wrapper: a transient error must not be fatal."""
    flaky = _Flaky(fail_times=2)
    slept = []

    with patch("modelq.app.redis_retry.time.sleep", slept.append):
        wrapped = _RedisWithRetry(flaky)
        assert wrapped.ping() is True

    assert flaky.calls == 3, "should have retried twice then succeeded"
    assert len(slept) == 2, "should sleep once per failed attempt"
    lo = _RedisWithRetry.RETRY_DELAY * (1 - _RedisWithRetry.RETRY_JITTER)
    hi = _RedisWithRetry.RETRY_DELAY * (1 + _RedisWithRetry.RETRY_JITTER)
    for s in slept:
        assert lo <= s <= hi
