import time
import redis
from redis.exceptions import ConnectionError, TimeoutError
import logging

logger = logging.getLogger(__name__)

class _RedisWithRetry:
    """Lightweight proxy that wraps a redis.Redis instance.

    Any callable attribute (e.g. get, set, blpop, xadd …) is executed with a
    retry loop that catches *ConnectionError* and *TimeoutError* from redis‑py
    and re‑issues the call after a fixed delay. Retries indefinitely until
    the connection succeeds.
    """

    RETRYABLE = (ConnectionError, TimeoutError)
    RETRY_DELAY = 30  # seconds between retry attempts

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
                    logger.warning(
                        f"Redis '{name}' failed ({exc.__class__.__name__}: {exc}). "
                        f"Retrying in {self.RETRY_DELAY}s (attempt {attempt})")
                    time.sleep(self.RETRY_DELAY)
        return _wrapped