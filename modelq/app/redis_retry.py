import time
import redis
from redis.exceptions import ConnectionError, TimeoutError
import logging

logger = logging.getLogger(__name__)
import random

class _RedisWithRetry:
    """Lightweight proxy that wraps a redis.Redis instance.

    Any callable attribute (e.g. get, set, blpop, xadd …) is executed with a
    retry loop that catches *ConnectionError* and *TimeoutError* from redis‑py
    and re‑issues the call after an exponential back‑off (base_delay × backoff^n)
    plus a small random jitter.
    """

    RETRYABLE = (ConnectionError, TimeoutError)

    def __init__(self, client: redis.Redis, *,
                 max_attempts: int = 5,
                 base_delay: float = 0.5,
                 backoff: float = 2.0,
                 jitter: float = 0.3):
        self._client = client
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._backoff = backoff
        self._jitter = jitter

    # Forward non‑callable attrs (e.g. "connection_pool") directly  ──────────
    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        # Wrap callable with retry loop
        def _wrapped(*args, **kwargs):
            attempt = 0
            delay   = self._base_delay
            while True:
                try:
                    return attr(*args, **kwargs)
                except self.RETRYABLE as exc:
                    attempt += 1
                    if attempt >= self._max_attempts:
                        logger.error(
                            f"Redis command '{name}' failed after {attempt} attempts: {exc}")
                        raise
                    sleep_for = delay + random.uniform(0, self._jitter)
                    logger.warning(
                        f"Redis '{name}' failed ({exc.__class__.__name__}: {exc}). "
                        f"Retrying in {sleep_for:.2f}s  (attempt {attempt}/{self._max_attempts})")
                    time.sleep(sleep_for)
                    delay *= self._backoff
        return _wrapped