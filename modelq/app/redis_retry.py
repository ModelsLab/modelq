import random
import time
import redis
from redis.exceptions import ConnectionError, TimeoutError
import logging

logger = logging.getLogger(__name__)

class _RedisWithRetry:
    """Lightweight proxy that wraps a redis.Redis instance.

    Any callable attribute (e.g. get, set, blpop, xadd …) is executed with a
    retry loop that catches *ConnectionError* and *TimeoutError* from redis‑py
    and re‑issues the call after a short, jittered delay. Retries indefinitely
    until the connection succeeds.
    """

    RETRYABLE = (ConnectionError, TimeoutError)
    # Recovery from a dead connection costs socket_timeout (detect) + this
    # (wait), so this value is the tail of every stall. Kept short.
    RETRY_DELAY = 15  # seconds between retry attempts
    # Connection loss is a SHARED event, not an independent one: on 2026-08-15
    # every worker on a node logged the same failure in the same second,
    # because one upstream path change broke all their connections at once. A
    # fixed delay would march that whole herd back into Redis in lockstep.
    # Jitter spreads the reconnect over a window instead. Set to 0 for
    # deterministic delays in tests.
    RETRY_JITTER = 0.5  # +/- this fraction of RETRY_DELAY

    @classmethod
    def _next_delay(cls) -> float:
        """RETRY_DELAY spread over [1-jitter, 1+jitter] of itself."""
        if not cls.RETRY_JITTER:
            return cls.RETRY_DELAY
        low, high = 1.0 - cls.RETRY_JITTER, 1.0 + cls.RETRY_JITTER
        return cls.RETRY_DELAY * random.uniform(low, high)

    def __init__(self, client: redis.Redis):
        self._client = client

    # Forward non‑callable attrs (e.g. "connection_pool") directly  ──────────
    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        # Wrap callable with retry loop
        def _wrapped(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return attr(*args, **kwargs)
                except self.RETRYABLE as exc:
                    attempt += 1
                    delay = self._next_delay()
                    logger.warning(
                        f"Redis '{name}' failed ({exc.__class__.__name__}: {exc}). "
                        f"Retrying in {delay:.1f}s (attempt {attempt})")
                    time.sleep(delay)
        return _wrapped