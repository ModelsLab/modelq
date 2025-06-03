import redis
import json
import functools
import threading
import time
import uuid
import logging
import traceback
from typing import Optional, Dict, Any
import socket

import requests  # For sending error payloads to a webhook

from modelq.app.tasks import Task
from modelq.exceptions import TaskProcessingError, TaskTimeoutError,RetryTaskException
from modelq.app.middleware import Middleware

from pydantic import BaseModel, ValidationError 
from typing import Optional, Dict, Any, Type 
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ModelQ:
    # Constants for heartbeat and pruning intervals
    HEARTBEAT_INTERVAL = 30     # seconds: how often this server updates its heartbeat
    PRUNE_TIMEOUT = 300         # seconds: how long before a server is considered stale
    PRUNE_CHECK_INTERVAL = 60   # seconds: how often to check for stale servers
    TASK_RESULT_RETENTION = 86400

    def __init__(
        self,
        host: str = "localhost",
        server_id: Optional[str] = None,
        username: str = None,
        port: int = 6379,
        db: int = 0,
        password: str = None,
        ssl: bool = False,
        ssl_cert_reqs: Any = None,
        redis_client: Any = None,
        max_connections: int = 50,  # Limit max connections to avoid "too many clients"
        webhook_url: Optional[str] = None,  # Optional webhook for error logging
        requeue_threshold : Optional[int] = None ,
        delay_seconds: int = 30,
        **kwargs,
    ):
        if redis_client:
            self.redis_client = redis_client
        else:
            self.redis_client = self._connect_to_redis(
                host=host,
                port=port,
                db=db,
                password=password,
                username=username,
                ssl=ssl,
                ssl_cert_reqs=ssl_cert_reqs,
                max_connections=max_connections,
                **kwargs,
            )

        self.worker_threads = []
        if server_id is None:
            # Attempt to load the server_id from a local file:
            server_id = self._get_or_create_server_id_file()
        self.server_id = server_id
        self.allowed_tasks = set()
        self.middleware: Middleware = None
        self.webhook_url = webhook_url
        self.requeue_threshold = requeue_threshold
        self.delay_seconds = delay_seconds

        # Register this server in Redis (with an initial heartbeat)
        self.register_server()

    def _connect_to_redis(
        self,
        host: str,
        port: int,
        db: int,
        password: str,
        ssl: bool,
        ssl_cert_reqs: Any,
        username: str,
        max_connections: int = 50,
        **kwargs,
    ) -> redis.Redis:
        pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            username=username,
            # Enable TLS/SSL if needed:
            # ssl=ssl,
            # ssl_cert_reqs=ssl_cert_reqs,
            max_connections=max_connections,
        )
        return redis.Redis(connection_pool=pool)

    def _get_or_create_server_id_file(self) -> str:
        return str(socket.gethostname())

    def register_server(self):
        """
        Registers this server in the 'servers' hash, including allowed tasks,
        current status, and last_heartbeat timestamp.
        """
        server_data = {
            "allowed_tasks": list(self.allowed_tasks),
            "status": "idle",
            "last_heartbeat": time.time(),
        }
        self.redis_client.hset("servers", self.server_id, json.dumps(server_data))

    def requeue_stuck_processing_tasks(self, threshold: float = 180.0):
        """
        Re-queues any tasks that have been in 'processing' for more than 'threshold' seconds.
        """

        if self.requeue_threshold :
            threshold = self.requeue_threshold

        processing_task_ids = self.redis_client.smembers("processing_tasks")
        now = time.time()

        for pid in processing_task_ids:
            task_id = pid.decode("utf-8")
            task_data = self.redis_client.get(f"task:{task_id}")
            if not task_data:
                # If there's no data in Redis for that task, remove it from processing set.
                self.redis_client.srem("processing_tasks", task_id)
                logger.warning(
                    f"No record found for in-progress task {task_id}. Removing from 'processing_tasks'."
                )
                continue

            task_dict = json.loads(task_data)
            started_at = task_dict.get("started_at", 0)
            if started_at:
                if now - started_at > threshold:
                    logger.info(
                        f"Re-queuing stuck task {task_id} which has been 'processing' for {now - started_at:.2f} seconds."
                    )
                    # Update status, queued_at, etc.
                    task_dict["status"] = "queued"
                    task_dict["queued_at"] = now
    
                    # Store the updated dict back in Redis
                    self.redis_client.set(f"task:{task_id}", json.dumps(task_dict),ex=86400)
                    
                    # Push it back into ml_tasks
                    self.redis_client.rpush("ml_tasks", json.dumps(task_dict))
    
                    # Remove from processing set
                    self.redis_client.srem("processing_tasks", task_id)

    def prune_old_task_results(self, older_than_seconds: int = None):
        """
        Deletes task result keys (stored with the prefix 'task_result:') whose
        finished_at (or started_at if finished_at is not available) timestamp is older
        than `older_than_seconds`. In addition, it also removes the corresponding
        task key (stored with the prefix 'task:').
        """
        if older_than_seconds is None:
            older_than_seconds = self.TASK_RESULT_RETENTION

        now = time.time()
        keys_deleted = 0

        # Use scan_iter to avoid blocking Redis
        for key in self.redis_client.scan_iter("task_result:*"):
            try:
                task_json = self.redis_client.get(key)
                if not task_json:
                    continue
                task_data = json.loads(task_json)
                # Use finished_at if available; otherwise fallback to started_at
                timestamp = task_data.get("finished_at") or task_data.get("started_at")
                if timestamp and (now - timestamp > older_than_seconds):
                    # Delete the task_result key
                    self.redis_client.delete(key)
                    # Extract the task id from the key and delete the corresponding task key.
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                    task_id = key_str.split("task_result:")[-1]
                    task_key = f"task:{task_id}"
                    self.redis_client.delete(task_key)
                    keys_deleted += 1
                    logger.info(f"Deleted old keys: {key_str} and {task_key}")
            except Exception as e:
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                logger.error(f"Error processing key {key_str}: {e}")

        if keys_deleted:
            logger.info(f"Pruned {keys_deleted} task(s) older than {older_than_seconds} seconds.")
            
    def update_server_status(self, status: str):
        """
        Updates the server's status in Redis.
        """
        raw_data = self.redis_client.hget("servers", self.server_id)
        if not raw_data:
            self.register_server()
            return
        server_data = json.loads(raw_data)
        server_data["status"] = status
        server_data["last_heartbeat"] = time.time()
        self.redis_client.hset("servers", self.server_id, json.dumps(server_data))

    def get_registered_server_ids(self) -> list:
        """
        Returns a list of server_ids that are currently registered in Redis under the 'servers' hash.
        """
        keys = self.redis_client.hkeys("servers")  # returns raw bytes for each key
        return [k.decode("utf-8") for k in keys]

    def heartbeat(self):
        """
        Periodically update this server's 'last_heartbeat' in Redis.
        """
        raw_data = self.redis_client.hget("servers", self.server_id)
        if not raw_data:
            self.register_server()
            return

        data = json.loads(raw_data)
        data["last_heartbeat"] = time.time()
        self.redis_client.hset("servers", self.server_id, json.dumps(data))

    def prune_inactive_servers(self, timeout_seconds: int = None):
        """
        Removes servers from the 'servers' hash if they haven't sent
        a heartbeat within 'timeout_seconds' seconds.
        """
        if timeout_seconds is None:
            timeout_seconds = self.PRUNE_TIMEOUT

        all_servers = self.redis_client.hgetall("servers")
        now = time.time()
        removed_count = 0

        for server_id_bytes, data_bytes in all_servers.items():
            server_id_str = server_id_bytes.decode("utf-8")
            try:
                data = json.loads(data_bytes.decode("utf-8"))
                last_heartbeat = data.get("last_heartbeat", 0)
                if (now - last_heartbeat) > timeout_seconds:
                    self.redis_client.hdel("servers", server_id_str)
                    removed_count += 1
                    logger.info(f"[Prune] Removed stale server: {server_id_str}")
            except Exception as e:
                logger.warning(f"[Prune] Could not parse server data for {server_id_str}: {e}")

        if removed_count > 0:
            logger.info(f"[Prune] Total {removed_count} inactive servers pruned.")

    def enqueue_task(self, task_data: dict, payload: dict):
        """
        Pushes a task into the 'ml_tasks' list with status=queued.
        We assume 'task_data' may already have 'created_at' set.
        Here, we optionally set 'queued_at' if not present.
        """
        # Ensure status is 'queued'
        task_data["status"] = "queued"
        self.check_middleware("before_enqueue")
        # If the decorator didn’t set queued_at, set it now
        if "queued_at" not in task_data:
            task_data["queued_at"] = time.time()

        self.redis_client.rpush("ml_tasks", json.dumps(task_data))
        self.redis_client.zadd("queued_requests", {task_data["task_id"]: task_data["queued_at"]})
        self.check_middleware("after_enqueue")

    def delete_queue(self):
        self.redis_client.ltrim("ml_tasks", 1, 0)
        
    def enqueue_delayed_task(self, task_dict: dict, delay_seconds: int):
        """
        Enqueues a task into a Redis sorted set ('delayed_tasks') to be processed later.
        """
        run_at = time.time() + delay_seconds
        task_json = json.dumps(task_dict)
        self.redis_client.zadd("delayed_tasks", {task_json: run_at})
        logger.info(f"Delayed task {task_dict.get('task_id')} by {delay_seconds} seconds.")

    def requeue_delayed_tasks(self):
        """
        Thread that periodically checks 'delayed_tasks' for tasks whose run_at time has passed,
        then moves them into 'ml_tasks' for immediate processing.
        """
        while True:
            now = time.time()
            ready_tasks = self.redis_client.zrangebyscore("delayed_tasks", 0, now)
            for task_json in ready_tasks:
                self.redis_client.zrem("delayed_tasks", task_json)
                self.redis_client.lpush("ml_tasks", task_json)
            time.sleep(1)

    def requeue_inprogress_tasks(self):
        """
        On server startup, re-queue tasks that were marked 'processing' but never finished.
        """
        logger.info("Checking for in-progress tasks to re-queue on startup...")
        processing_task_ids = self.redis_client.smembers("processing_tasks")
        for pid in processing_task_ids:
            task_id = pid.decode("utf-8")
            task_data = self.redis_client.get(f"task:{task_id}")
            if not task_data:
                self.redis_client.srem("processing_tasks", task_id)
                logger.warning(f"No record found for in-progress task {task_id}. Removing it.")
                continue

            task_dict = json.loads(task_data)
            if task_dict.get("status") == "processing":
                logger.info(f"Re-queuing task {task_id} which was in progress.")
                task_dict["payload"] = task_dict.original_payload
                self.redis_client.rpush("ml_tasks", json.dumps(task_dict))
                self.redis_client.srem("processing_tasks", task_id)

    def get_all_queued_tasks(self) -> list:
        """
        Returns a list of tasks currently in the 'ml_tasks' list with a status of 'queued'.
        """
        queued_tasks = []
        tasks_in_list = self.redis_client.lrange("ml_tasks", 0, -1)

        for t_json in tasks_in_list:
            try:
                t_dict = json.loads(t_json)
                if t_dict.get("status") == "queued":
                    queued_tasks.append(t_dict)
            except Exception as e:
                logger.error(f"Error deserializing task from ml_tasks: {e}")

        return queued_tasks

    def task(
        self,
        task_class=Task,
        timeout: Optional[int] = None,
        stream: bool = False,
        retries: int = 0,
        schema: Optional[Type[BaseModel]] = None,        # ▶ pydantic
        returns: Optional[Type[BaseModel]] = None,       # ▶ pydantic
    ):
        def decorator(func):
            # make the schema classes discoverable at run time
            func._mq_schema  = schema                    # ▶ pydantic
            func._mq_returns = returns                   # ▶ pydantic

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # ---------------------------  PRODUCER-SIDE VALIDATION
                if schema is not None:                   # ▶ pydantic
                    try:
                        # allow either a ready-made model instance
                        # or raw kwargs/args that build one
                        if len(args) == 1 and isinstance(args[0], schema):
                            validated = args[0]
                        else:
                            validated = schema(*args, **kwargs)
                    except ValidationError as ve:
                        raise TaskProcessingError(
                            func.__name__, f"Input validation failed – {ve}"
                        )
                    payload_data = validated.model_dump(mode="json")  # zero-copy
                    args, kwargs = (), {}          # we’ll carry payload in kwargs only
                else:
                    payload_data = {"args": args, "kwargs": kwargs}

                payload = {
                    "data": payload_data,          # ▶ pydantic – typed or raw
                    "timeout": timeout,
                    "stream": stream,
                    "retries": retries,
                }

                task = task_class(task_name=func.__name__, payload=payload)
                if stream:
                    task.stream = True

                task_dict = task.to_dict()
                now_ts = time.time()
                task_dict["created_at"] = now_ts
                task_dict["queued_at"]  = now_ts

                self.enqueue_task(task_dict, payload=payload)
                self.redis_client.set(f"task:{task.task_id}",
                                      json.dumps(task_dict),
                                      ex=86400)
                return task
            setattr(self, func.__name__, func)
            self.allowed_tasks.add(func.__name__)
            self.register_server()
            return wrapper
        return decorator

    def start_workers(self, no_of_workers: int = 1):
        """
        Starts worker threads to pop tasks from 'ml_tasks' and process them.
        Also starts:
            - a thread for re-queuing delayed tasks
            - a heartbeat thread
            - a pruning thread
        """
        # Avoid restarting if workers are already running
        if any(thread.is_alive() for thread in self.worker_threads):
            return

        self.check_middleware("before_worker_boot")

        # 1) Delayed re-queue thread
        requeue_thread = threading.Thread(target=self.requeue_delayed_tasks, daemon=True)
        requeue_thread.start()
        self.worker_threads.append(requeue_thread)

        # 2) Heartbeat thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        self.worker_threads.append(heartbeat_thread)

        # 3) Pruning thread
        pruning_thread = threading.Thread(target=self._pruning_loop, daemon=True)
        pruning_thread.start()
        self.worker_threads.append(pruning_thread)

        # 4) Worker threads
        def worker_loop(worker_id):
            self.check_middleware("after_worker_boot")
            while True:
                try:
                    self.update_server_status(f"worker_{worker_id}: idle")
                    task_data = self.redis_client.blpop("ml_tasks")  # blocks until a task is available
                    if not task_data:
                        continue

                    self.update_server_status(f"worker_{worker_id}: busy")
                    _, task_json = task_data
                    task_dict = json.loads(task_json)
                    task = Task.from_dict(task_dict)

                    # Mark task as 'processing'
                    added = self.redis_client.sadd("processing_tasks", task.task_id)
                    if added == 0:
                        logger.warning(
                            f"Task {task.task_id} is already being processed. Skipping duplicate."
                        )
                        continue
                    task.status = "processing"

                    # Set started_at
                    task_dict["started_at"] = time.time()

                    # Update in Redis
                    self.redis_client.set(f"task:{task.task_id}", json.dumps(task_dict),ex=86400)

                    if task.task_name in self.allowed_tasks:
                        try:
                            logger.info(f"Worker {worker_id} started processing: {task.task_name}")
                            start_time = time.time()
                            self.process_task(task)
                            end_time = time.time()
                            logger.info(
                                f"Worker {worker_id} finished {task.task_name} "
                                f"in {end_time - start_time:.2f} seconds"
                            )

                        except TaskProcessingError as e:
                            logger.error(
                                f"Worker {worker_id} encountered a TaskProcessingError: {e}"
                            )
                            if task.payload.get("retries", 0) > 0:
                                new_task_dict = task.to_dict()
                                new_task_dict["payload"] = task.original_payload
                                new_task_dict["payload"]["retries"] -= 1
                                self.enqueue_delayed_task(new_task_dict, delay_seconds=self.delay_seconds)

                        except Exception as e:
                            logger.error(
                                f"Worker {worker_id} encountered an unexpected error: {e}"
                            )
                            if task.payload.get("retries", 0) > 0:
                                new_task_dict = task.to_dict()
                                new_task_dict["payload"] = task.original_payload
                                new_task_dict["payload"]["retries"] -= 1
                                self.enqueue_delayed_task(new_task_dict, delay_seconds=self.delay_seconds)
                    else:
                        # If task is not allowed on this server, re-queue it
                        logger.warning(
                            f"Worker {worker_id} cannot process task {task.task_name}, re-queueing..."
                        )
                        self.redis_client.rpush("ml_tasks", task_json)

                except Exception as e:
                    logger.error(
                        f"Worker {worker_id} crashed with error: {e}. Restarting worker..."
                    )
                finally:
                    self.check_middleware("before_worker_shutdown")
                    self.check_middleware("after_worker_shutdown")

        for i in range(no_of_workers):
            worker_thread = threading.Thread(target=worker_loop, args=(i,), daemon=True)
            worker_thread.start()
            self.worker_threads.append(worker_thread)

        task_names = ", ".join(self.allowed_tasks) if self.allowed_tasks else "No tasks registered"
        logger.info(
            f"ModelQ workers started with {no_of_workers} worker(s). "
            f"Connected to Redis at {self.redis_client.connection_pool.connection_kwargs['host']}:"
            f"{self.redis_client.connection_pool.connection_kwargs['port']}. "
            f"Registered tasks: {task_names}"
        )

    def _heartbeat_loop(self):
        """
        Continuously updates the heartbeat for this server.
        """
        while True:
            self.heartbeat()
            time.sleep(self.HEARTBEAT_INTERVAL)

    def _pruning_loop(self):
        """
        Continuously prunes servers that have not updated their heartbeat in a while.
        """
        while True:
            self.prune_inactive_servers(timeout_seconds=self.PRUNE_TIMEOUT)
            self.requeue_stuck_processing_tasks(threshold=180)
            self.prune_old_task_results(older_than_seconds=self.TASK_RESULT_RETENTION)
            time.sleep(self.PRUNE_CHECK_INTERVAL)

    def check_middleware(self, middleware_event: str,task: Optional[Task] = None, error: Optional[Exception] = None):
        """
        Hooks into the Middleware lifecycle if a Middleware instance is attached.
        """
        if self.middleware:
            self.middleware.execute(event=middleware_event,task=task, error=error)

    def process_task(self, task: Task) -> None:
        """
        Processes the task by invoking the registered function. Handles timeouts, streaming,
        error logging, and error reporting to a webhook.
        We'll set finished_at on success or fail.
        """
        try:
            if task.task_name not in self.allowed_tasks:
                task.status = "failed"
                task.result = "Task not allowed on this server."
                self._store_final_task_state(task)
                logger.error(f"Task {task.task_name} is not allowed on this server.")
                raise TaskProcessingError(task.task_name, "Task not allowed")

            task_function = getattr(self, task.task_name, None)
            if not task_function:
                task.status = "failed"
                task.result = "Task function not found"
                self._store_final_task_state(task)
                logger.error(f"Task {task.task_name} failed - function not found.")
                raise TaskProcessingError(task.task_name, "Task function not found")

            # ---- New: Check for Pydantic schema
            schema_cls  = getattr(task_function, "_mq_schema",  None)
            return_cls  = getattr(task_function, "_mq_returns", None)

            # ---- Prepare args/kwargs based on schema
            if schema_cls is not None:
                try:
                    # Accept either dict or JSON-serialized dict
                    payload_data = task.payload["data"]
                    if isinstance(payload_data, str):
                        import json
                        payload_data = json.loads(payload_data)
                    validated_in = schema_cls(**payload_data)
                except Exception as ve:
                    task.status = "failed"
                    task.result = f"Input validation failed – {ve}"
                    self._store_final_task_state(task, success=False)
                    logger.error(f"[ModelQ] Input validation failed: {ve}")
                    raise TaskProcessingError(task.task_name, f"Input validation failed: {ve}")
                call_args = (validated_in,)
                call_kwargs = {}
            else:
                # Legacy: no schema
                call_args = tuple(task.payload['data'].get("args", ()))
                call_kwargs = dict(task.payload['data'].get("kwargs", {}))
            timeout = task.payload.get("timeout", None)
            stream = task.payload.get("stream", False)

            logger.info(
                f"Processing task: {task.task_name} "
                f"with args: {call_args}, kwargs: {call_kwargs}"
            )

            if stream:
                # Stream results
                for result in task_function(*call_args, **call_kwargs):
                    import json
                    task.status = "in_progress"
                    self.redis_client.xadd(
                        f"task_stream:{task.task_id}",
                        {"result": json.dumps(result, default=str)}
                    )
                # Once streaming is done
                task.status = "completed"
                self.redis_client.expire(f"task_stream:{task.task_id}", 3600)
                self._store_final_task_state(task, success=True)
            else:
                # Standard execution with optional timeout
                if timeout:
                    result = self._run_with_timeout(
                        task_function, timeout,
                        *call_args, **call_kwargs
                    )
                else:
                    result = task_function(
                        *call_args, **call_kwargs
                    )

                # ---- New: Output validation for standard result
                if return_cls is not None:
                    try:
                        if not isinstance(result, return_cls):
                            result = return_cls(**(result if isinstance(result, dict) else result.__dict__))
                    except Exception as ve:
                        task.status = "failed"
                        task.result = f"Output validation failed – {ve}"
                        self._store_final_task_state(task, success=False)
                        logger.error(f"[ModelQ] Output validation failed: {ve}")
                        raise TaskProcessingError(task.task_name, f"Output validation failed: {ve}")

                # When you set `task.result` (in process_task), use this logic:
                if isinstance(result, BaseModel):
                    # Pydantic object: store as dict, not string!
                    task.result = result.model_dump(mode="json")
                elif isinstance(result, (dict, list, int, float, bool)):
                    task.result = result
     # only images as base64 string
                else:
                    task.result = str(result)

                task.status = "completed"
                self._store_final_task_state(task, success=True)

            logger.info(f"Task {task.task_name} completed successfully.")

        except RetryTaskException as e:
            logger.warning(f"Task {task.task_name} requested retry: {e}")
            new_task_dict = task.to_dict()
            new_task_dict["payload"] = task.original_payload
            self.enqueue_delayed_task(new_task_dict, delay_seconds=self.delay_seconds)
        except Exception as e:
            # Mark as failed
            task.status = "failed"
            task.result = str(e)
            self._store_final_task_state(task, success=False)

            # 1) Log to file
            self.log_task_error_to_file(task, e)
            self.check_middleware("on_error", task=task, error=e)

            # 2) Webhook (if configured)
            self.post_error_to_webhook(task, e)
            logger.error(f"Task {task.task_name} failed with error: {e}")
            raise TaskProcessingError(task.task_name, str(e))

        finally:
            self.redis_client.srem("processing_tasks", task.task_id)


    def _store_final_task_state(self, task: Task, success: bool):
        """
        Persists the final status/result of the task in Redis, adding finished_at.
        """
        task_dict = task.to_dict()

        # Mark finished_at
        task_dict["finished_at"] = time.time()
        
        self.redis_client.set(
            f"task_result:{task.task_id}",
            json.dumps(task_dict),
            ex=3600,
        )
        self.redis_client.set(
            f"task:{task.task_id}",
            json.dumps(task_dict),
            ex=86400
        )

        
    def _run_with_timeout(self, func, timeout, *args, **kwargs):
        """
        Runs the given function with a threading-based timeout.
        If still alive after `timeout` seconds, raises TaskTimeoutError.
        """
        result = [None]
        exception = [None]

        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as ex:
                exception[0] = ex

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            logger.error(f"Task exceeded timeout of {timeout} seconds.")
            raise TaskTimeoutError(f"Task exceeded timeout of {timeout} seconds")
        if exception[0]:
            raise exception[0]
        return result[0]

    def get_task_status(self, task_id: str) -> Optional[str]:
        """
        Returns the stored status of a given task_id.
        """
        task_data = self.redis_client.get(f"task:{task_id}")
        if task_data:
            return json.loads(task_data).get("status")
        return None

    def log_task_error_to_file(self, task: Task, exc: Exception, file_path="modelq_errors.log"):
        """
        Logs detailed error info to a specified file, with dashes before and after.
        """
        error_trace = traceback.format_exc()
        log_data = {
            "task_id": task.task_id,
            "task_name": task.task_name,
            "payload": task.payload,
            "error_message": str(exc),
            "traceback": error_trace,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        }
        with open(file_path, "a", encoding="utf-8") as f:
            f.write("----\n")
            f.write(json.dumps(log_data, indent=2))
            f.write("\n----\n")

    def post_error_to_webhook(self, task: Task, exc: Exception):
        """
        Non-blocking method to POST a detailed error message to the configured webhook.
        """
        if not self.webhook_url:
            return

        full_tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        payload_str = json.dumps(task.payload, indent=2)

        content_str = (
            f"**Task Name**: {task.task_name}\n"
            f"**Task ID**: {task.task_id}\n"
            f"**Payload**:\n```json\n{payload_str}\n```\n"
            f"**Error Message**: {exc}\n"
            f"**Traceback**:\n```{full_tb}```"
        )

        t = threading.Thread(
            target=self._post_error_to_webhook_sync,
            args=(content_str,),
            daemon=True
        )
        t.start()

    def _post_error_to_webhook_sync(self, content_str: str):
        payload = {"content": content_str}
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            if resp.status_code >= 400:
                logger.error(
                    f"Failed to POST error to webhook. "
                    f"Status code: {resp.status_code}, Response: {resp.text}"
                )
        except Exception as e2:
            logger.error(f"Exception while sending error to webhook: {e2}")

    def remove_task_from_queue(self, task_id: str) -> bool:
        """
        Removes a task from the 'ml_tasks' queue using its task_id.
        Returns True if the task was found and removed, False otherwise.
        """
        tasks = self.redis_client.lrange("ml_tasks", 0, -1)
        removed = False
        for task_json in tasks:
            try:
                task_dict = json.loads(task_json)
                if task_dict.get("task_id") == task_id:
                    self.redis_client.lrem("ml_tasks", 1, task_json)
                    self.redis_client.zrem("queued_requests", task_id)
                    removed = True
                    logger.info(f"Removed task {task_id} from queue.")
                    break
            except Exception as e:
                logger.error(f"Failed to process task while trying to remove: {e}")
        return removed
    