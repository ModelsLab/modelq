import threading
import time
import json
import socket
import logging
from typing import Optional, Type, Any, Generator

import redis
from pydantic import BaseModel, ValidationError

from modelq.app.tasks import Task
from modelq.app.middleware import Middleware
from modelq.exceptions import (
    TaskProcessingError,
    TaskTimeoutError,
    RetryTaskException,
)
from modelq.app.backends.base import QueueBackend
from modelq.app.backends.redis import RedisQueueBackend

# ─────────────────────── Logger Setup ────────────────────────────────
logger = logging.getLogger("modelq")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class ModelQ:
    """Core orchestrator class with pluggable queue backend (Redis by default)."""

    HEARTBEAT_INTERVAL = 30
    PRUNE_TIMEOUT = 300
    PRUNE_CHECK_INTERVAL = 60
    TASK_RESULT_RETENTION = 86_400  # 24 h

    # ─────────────────────── Init / Backend ──────────────────────────
    def __init__(
        self,
        backend: Optional[QueueBackend] = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        username: str | None = None,
        ssl: bool = False,
        ssl_cert_reqs: Any = None,
        max_connections: int = 50,
        server_id: str | None = None,
        webhook_url: str | None = None,
        requeue_threshold: int | None = None,
        delay_seconds: int = 30,
    ):
        if backend is None:
            pool = redis.ConnectionPool(
                host=host,
                port=port,
                db=db,
                password=password,
                username=username,
                max_connections=max_connections,
            )
            backend = RedisQueueBackend(redis.Redis(connection_pool=pool))
            logger.info("Using default Redis backend @ %s:%s", host, port)
        else:
            logger.info("Using custom backend: %s", backend.__class__.__name__)
        self.backend = backend

        self.server_id: str = server_id or socket.gethostname()
        self.allowed_tasks: set[str] = set()
        self.middleware: Middleware | None = None
        self.webhook_url = webhook_url
        self.requeue_threshold = requeue_threshold
        self.delay_seconds = delay_seconds
        self.worker_threads: list[threading.Thread] = []

        self.backend.register_server(self.server_id, list(self.allowed_tasks))
        logger.info("Server registered with id=%s", self.server_id)

    # ─────────────────────── Decorator API ───────────────────────────
    def task(
        self,
        task_class: Type[Task] = Task,
        timeout: int | None = None,
        stream: bool = False,
        retries: int = 0,
        schema: Type[BaseModel] | None = None,
        returns: Type[BaseModel] | None = None,
    ):
        def decorator(func):
            func._mq_schema = schema
            func._mq_returns = returns

            def wrapper(*args, **kwargs):
                # ── Input validation ──
                if schema is not None:
                    try:
                        instance = (
                            args[0] if len(args) == 1 and isinstance(args[0], schema) else schema(*args, **kwargs)
                        )
                        payload_data = instance.model_dump(mode="json")
                        args, kwargs = (), {}
                    except ValidationError as ve:
                        logger.error("Validation failed for task %s: %s", func.__name__, ve)
                        raise TaskProcessingError(func.__name__, f"Input validation failed – {ve}")
                else:
                    payload_data = {"args": args, "kwargs": kwargs}

                payload = {
                    "data": payload_data,
                    "timeout": timeout,
                    "stream": stream,
                    "retries": retries,
                }
                task = task_class(task_name=func.__name__, payload=payload)
                now = time.time()
                task_dict = task.to_dict() | {"created_at": now, "queued_at": now, "status": "queued"}

                self.backend.enqueue_task(task_dict)
                logger.info("Enqueued task %s (id=%s)", task.task_name, task.task_id)

                # attach helpers for convenient retrieval
                task.backend = self.backend
                task._modelq_ref = self
                task.result_blocking = lambda timeout=None, returns=None: task.get_result(
                    self.backend,
                    timeout=timeout,
                    returns=returns,
                    modelq_ref=self,
                )
                return task

            setattr(self, func.__name__, func)
            self.allowed_tasks.add(func.__name__)
            self.backend.register_server(self.server_id, list(self.allowed_tasks))
            logger.debug("Task registered: %s", func.__name__)
            return wrapper

        return decorator

    # ─────────────────────── Worker Management ───────────────────────
    def start_workers(self, no_of_workers: int = 1):
        if any(t.is_alive() for t in self.worker_threads):
            logger.warning("Workers already running — skipping start")
            return

        logger.info("Booting %d worker(s)…", no_of_workers)

        # supportive threads
        t_delay = threading.Thread(target=self._delay_loop, daemon=True, name="mq-delay")
        t_delay.start(); self.worker_threads.append(t_delay)
        t_hb = threading.Thread(target=self._heartbeat_loop, daemon=True, name="mq-heartbeat")
        t_hb.start(); self.worker_threads.append(t_hb)
        t_prune = threading.Thread(target=self._prune_loop, daemon=True, name="mq-prune")
        t_prune.start(); self.worker_threads.append(t_prune)

        # worker threads
        for wid in range(no_of_workers):
            th = threading.Thread(target=self._worker_loop, args=(wid,), daemon=True, name=f"mq-worker-{wid}")
            th.start(); self.worker_threads.append(th)

        logger.info("ModelQ online — workers=%d server_id=%s", no_of_workers, self.server_id)

    # ─────────────────────── Internal Loops ──────────────────────────
    def _worker_loop(self, wid: int):
        logger.debug("Worker %d spawned", wid)
        while True:
            task_dict = self.backend.dequeue_task()
            if not task_dict:
                continue
            task = Task.from_dict(task_dict,backend=self.backend)
            if not self.backend.mark_processing(task.task_id):
                continue  # duplicate
            logger.info("[W%d] Started %s (id=%s)", wid, task.task_name, task.task_id)
            try:
                self._execute_task(task)
                logger.info("[W%d] Completed %s (id=%s)", wid, task.task_name, task.task_id)
            except Exception as exc:
                logger.error("[W%d] Failed %s (id=%s): %s", wid, task.task_name, task.task_id, exc)
            finally:
                self.backend.unmark_processing(task.task_id)

    def _delay_loop(self):
        while True:
            released = self.backend.dequeue_ready_delayed_tasks()
            if released:
                logger.debug("Moved %d delayed task(s) to queue", len(released))
            time.sleep(1)

    def _heartbeat_loop(self):
        while True:
            self.backend.update_server_status(self.server_id, "alive")
            logger.debug("Heartbeat sent for %s", self.server_id)
            time.sleep(self.HEARTBEAT_INTERVAL)

    def _prune_loop(self):
        while True:
            pruned = self.backend.prune_dead_servers(self.PRUNE_TIMEOUT)
            if pruned:
                logger.warning("Pruned stale servers: %s", pruned)
            removed = self.backend.prune_old_results(self.TASK_RESULT_RETENTION)
            if removed:
                logger.info("Pruned %d old task result(s)", removed)
            time.sleep(self.PRUNE_CHECK_INTERVAL)

    # ─────────────────────── Task Execution ──────────────────────────
    def _execute_task(self, task: Task):
        func = getattr(self, task.task_name, None)
        if not func:
            raise TaskProcessingError(task.task_name, "Task function not found")

        schema_cls = getattr(func, "_mq_schema", None)
        return_cls = getattr(func, "_mq_returns", None)

        # prepare args / kwargs
        if schema_cls:
            payload_data = task.payload["data"]
            if isinstance(payload_data, str):
                payload_data = json.loads(payload_data)
            call_args = (schema_cls(**payload_data),)
            call_kwargs = {}
        else:
            call_args = tuple(task.payload["data"].get("args", ()))
            call_kwargs = dict(task.payload["data"].get("kwargs", {}))

        # handle streaming tasks
        if task.payload.get("stream"):
            self._run_streaming_task(task, func, call_args, call_kwargs)
            return

        # run with optional timeout
        try:
            if t := task.payload.get("timeout"):
                result = self._run_with_timeout(func, t, *call_args, **call_kwargs)
            else:
                result = func(*call_args, **call_kwargs)

            if return_cls and not isinstance(result, return_cls):
                result = return_cls(**(result if isinstance(result, dict) else result.__dict__))

            serialized_result = result.model_dump(mode="json") if isinstance(result, BaseModel) else result
            task.status = "completed"; task.result = serialized_result
            self.backend.save_task_state(task.task_id, task.to_dict(), result=True)
        except RetryTaskException:
            logger.warning("Task %s requested retry", task.task_id)
            task.payload["retries"] -= 1
            self.backend.enqueue_delayed_task(task.to_dict(), self.delay_seconds)
        except Exception as exc:
            task.status = "failed"; task.result = str(exc)
            self.backend.save_task_state(task.task_id, task.to_dict(), result=False)
            logger.exception("Task %s failed: %s", task.task_id, exc)
            raise

    # ─────────────────────── Streaming Helper ────────────────────────
    def _run_streaming_task(self, task: Task, func, call_args, call_kwargs):
        if not isinstance(self.backend, RedisQueueBackend):
            raise NotImplementedError("Streaming tasks currently require Redis backend")
        redis_client = self.backend.redis
        stream_key = f"task_stream:{task.task_id}"
        try:
            for chunk in func(*call_args, **call_kwargs):
                redis_client.xadd(stream_key, {"result": json.dumps(chunk, default=str)})
            task.status = "completed"; task.result = "<streamed>";
            redis_client.expire(stream_key, 3600)
            self.backend.save_task_state(task.task_id, task.to_dict(), result=True)
        except Exception as exc:
            task.status = "failed"; task.result = str(exc)
            self.backend.save_task_state(task.task_id, task.to_dict(), result=False)
            logger.exception("Streaming task %s failed: %s", task.task_id, exc)
            raise

    # ─────────────────────── Helpers ─────────────────────────────────
    @staticmethod
    def _run_with_timeout(func, timeout, *args, **kwargs):
        res = [None]; exc = [None]

        def target():
            try:
                res[0] = func(*args, **kwargs)
            except Exception as e:
                exc[0] = e

        th = threading.Thread(target=target)
        th.start(); th.join(timeout)
        if th.is_alive():
            raise TaskTimeoutError(func.__name__, timeout)