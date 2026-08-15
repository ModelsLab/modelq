"""Regression tests for the 2026-08-15 silent worker stall.

A dropped TCP connection to Redis left every worker parked forever in an
unbounded BLPOP: a pure read-wait transmits nothing, so the kernel never
retransmits, never gives up, and never raises. The process stayed up, HTTP
health checks kept passing, and the queue grew unattended for hours.

These tests pin the three properties that make that impossible now:
  1. every queue read is bounded by a timeout,
  2. the connection pool can detect a half-open socket,
  3. a background loop outlives an exception thrown by its body.
"""

import socket
import threading
import time
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from modelq import ModelQ
from modelq.app.base import _tcp_keepalive_options


@pytest.fixture
def modelq_instance():
    return ModelQ(redis_client=fakeredis.FakeStrictRedis())


# ---------------------------------------------------------------------------
# 1. the queue read must be bounded
# ---------------------------------------------------------------------------

def test_worker_blpop_passes_a_timeout(modelq_instance):
    """The worker must never issue an unbounded BLPOP.

    This is the actual outage. `blpop(key)` with no timeout blocks forever on a
    socket Redis has already forgotten.
    """
    seen = {}
    stop = threading.Event()

    def fake_blpop(key, *args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        stop.set()
        # Behave like a real timed-out BLPOP so the worker loops rather than
        # trying to decode a task.
        time.sleep(0.01)
        return None

    modelq_instance.redis_client = MagicMock(wraps=modelq_instance.redis_client)
    modelq_instance.redis_client.blpop.side_effect = fake_blpop

    modelq_instance.start_workers(no_of_workers=1)
    assert stop.wait(timeout=5), "worker never called blpop"

    timeout = seen["kwargs"].get("timeout", seen["args"][0] if seen["args"] else None)
    assert timeout is not None, "BLPOP was issued without a timeout"
    assert timeout > 0, f"BLPOP timeout must be positive, got {timeout!r}"


def test_blpop_timeout_stays_below_socket_timeout():
    """redis-py applies the socket read timeout to blocking commands too.

    If BLPOP_TIMEOUT ever crept above SOCKET_TIMEOUT, every idle poll would
    raise instead of returning None — turning an idle worker into a log flood.
    """
    assert ModelQ.BLPOP_TIMEOUT < ModelQ.SOCKET_TIMEOUT


# ---------------------------------------------------------------------------
# 2. the pool must be able to notice a dead socket
# ---------------------------------------------------------------------------

def test_connection_pool_enables_half_open_detection():
    with patch("modelq.app.base.redis.ConnectionPool") as pool, \
         patch("modelq.app.base.redis.Redis"):
        ModelQ(host="redis.invalid", port=6379, password="x", username=None)

    kwargs = pool.call_args.kwargs
    assert kwargs["socket_keepalive"] is True
    assert kwargs["health_check_interval"] > 0
    assert kwargs["socket_timeout"] == ModelQ.SOCKET_TIMEOUT
    assert kwargs["socket_connect_timeout"] == ModelQ.SOCKET_CONNECT_TIMEOUT
    assert kwargs["retry_on_timeout"] is True


def test_keepalive_options_are_valid_for_this_platform():
    """Every key must be a real socket constant, or the pool raises at connect."""
    options = _tcp_keepalive_options()
    valid = {
        getattr(socket, name)
        for name in ("TCP_KEEPIDLE", "TCP_KEEPALIVE", "TCP_KEEPINTVL", "TCP_KEEPCNT")
        if hasattr(socket, name)
    }
    assert set(options).issubset(valid)
    assert all(isinstance(v, int) and v > 0 for v in options.values())


# ---------------------------------------------------------------------------
# 3. a background loop must survive a transient error
# ---------------------------------------------------------------------------

def test_guarded_iteration_swallows_and_continues(modelq_instance):
    modelq_instance.BACKGROUND_LOOP_BACKOFF = 0
    with modelq_instance._guarded_iteration("unit-test"):
        raise ConnectionError("transient redis blip")
    # Reaching here at all is the assertion: the exception did not propagate.


def test_heartbeat_loop_survives_a_raising_body(modelq_instance):
    """The pruning/heartbeat threads died outright on 2026-08-15.

    One raise used to unwind the thread permanently. Now the loop must still be
    calling its body after the failure.
    """
    modelq_instance.BACKGROUND_LOOP_BACKOFF = 0
    modelq_instance.HEARTBEAT_INTERVAL = 0.01
    calls = []
    done = threading.Event()

    def exploding_heartbeat():
        calls.append(time.time())
        if len(calls) == 1:
            raise ConnectionError("transient redis blip")
        if len(calls) >= 3:
            done.set()

    modelq_instance.heartbeat = exploding_heartbeat
    threading.Thread(target=modelq_instance._heartbeat_loop, daemon=True).start()

    assert done.wait(timeout=5), (
        f"loop stopped after {len(calls)} call(s); it died on the first raise"
    )
