import redis
import json
import functools
import threading
import time
import uuid
import logging
import traceback
from typing import Optional, Dict, Any

from modelq.app.tasks import Task
from modelq.exceptions import TaskProcessingError, TaskTimeoutError
from modelq.app.middleware import Middleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ModelQ:
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
        max_connections: int = 50,
        # NOTE: We remove the process-based concurrency to avoid pickle issues
        **kwargs,
    ):
        """
        ModelQ constructor. Initializes Redis client.
        """
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

        # We no longer use a ProcessPoolExecutor. We'll do thread-based timeout checks.
        self.worker_threads = []
        self.server_id = server_id or str(uuid.uuid4())
        self.allowed_tasks = set()
        self.task_configurations: Dict[str, Dict[str, Any]] = {}

        # Optional middleware
        self.middleware: Middleware = None

        # Register this server in Redis
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
        """
        Connect to Redis using a connection pool.
        """
        pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            username=username,
            # ssl=ssl,
            # ssl_cert_reqs=ssl_cert_reqs,
            max_connections=max_connections,
        )
        return redis.Redis(connection_pool=pool)

    def register_server(self):
        """
        Register or update server in Redis under "servers" hash.
        """
        self.redis_client.hset(
            "servers",
            self.server_id,
            json.dumps({"allowed_tasks": list(self.allowed_tasks), "status": "idle"}),
        )

    def update_server_status(self, status: str):
        """
        Update server's status ("idle", "busy", etc.) in Redis.
        """
        server_data_raw = self.redis_client.hget("servers", self.server_id)
        if server_data_raw:
            server_data = json.loads(server_data_raw)
        else:
            server_data = {}

        server_data["status"] = status
        self.redis_client.hset("servers", self.server_id, json.dumps(server_data))

    def enqueue_task(self, task_name: dict, payload: dict):
        """
        Enqueue a task into the Redis list "ml_tasks" if not already queued.
        task_name is actually a dictionary, from task.to_dict().
        """
        task = {**task_name, "status": "queued"}
        task_id = task.get("task_id")

        if not self._is_task_in_queue(task_id):
            with self.redis_client.pipeline() as pipe:
                pipe.rpush("ml_tasks", json.dumps(task))
                pipe.sadd("queued_tasks", task_id)  # track queued tasks in a set
                pipe.execute()
        else:
            logger.warning(f"Task {task_id} is already in the queue, skipping enqueue.")

    def _is_task_in_queue(self, task_id: str) -> bool:
        """
        Check if a task ID is in "queued_tasks" set. O(1) membership test.
        """
        return self.redis_client.sismember("queued_tasks", task_id)

    def get_all_queued_tasks(self) -> list:
        """
        Example method to return all queued tasks (in 'queued_tasks' set).
        """
        queued_task_ids = self.redis_client.smembers("queued_tasks")
        queued_tasks = []
        if queued_task_ids:
            with self.redis_client.pipeline() as pipe:
                for t_id in queued_task_ids:
                    pipe.get(f"task:{t_id.decode('utf-8')}")
                results = pipe.execute()

            for res in results:
                if res:
                    task_data = json.loads(res)
                    if task_data.get("status") == "queued":
                        queued_tasks.append(task_data)
        return queued_tasks

    def is_task_processing_or_executed(self, task_id: str) -> bool:
        """
        Check if a task is already processing or completed by looking up its status.
        """
        task_status = self.get_task_status(task_id)
        return task_status in ["processing", "completed"]

    def task(
        self,
        task_class=Task,
        timeout: Optional[int] = None,
        stream: bool = False,
        retries: int = 0,
    ):
        """
        Decorator to define a task. For example:

        @modelq.task(timeout=30, retries=1)
        def my_task_function(x, y):
            return x + y
        """
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                task_name = func.__name__
                payload = {
                    "args": args,
                    "kwargs": kwargs,
                    "timeout": timeout,
                    "stream": stream,
                    "retries": retries,
                }
                task = task_class(task_name=task_name, payload=payload)
                if stream:
                    task.stream = True
                # Enqueue and store in Redis
                self.enqueue_task(task.to_dict(), payload=payload)
                self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                return task

            # Register this function name as an allowed task
            setattr(self, func.__name__, func)
            self.allowed_tasks.add(func.__name__)
            self.register_server()

            return wrapper

        return decorator

    def start_workers(self, no_of_workers: int = 1):
        """
        Start worker threads which pop tasks from "ml_tasks" queue.
        We'll do short blocking intervals to make pickup more responsive.
        """
        # Ensure we only start workers once
        if any(thread.is_alive() for thread in self.worker_threads):
            return

        self.check_middleware("before_worker_boot")

        def worker_loop(worker_id):
            while True:
                try:
                    self.update_server_status(f"worker_{worker_id}: idle")

                    # Short-block on "ml_tasks" for 2 seconds
                    task_data = self.redis_client.blpop("ml_tasks", timeout=2)
                    if not task_data:
                        # No task popped in 2s, loop again
                        continue

                    self.update_server_status(f"worker_{worker_id}: busy")
                    _, task_json = task_data
                    task_dict = json.loads(task_json)
                    task = Task.from_dict(task_dict)

                    # Remove from queued set
                    self.redis_client.srem("queued_tasks", task.task_id)

                    if task.task_name in self.allowed_tasks:
                        try:
                            logger.info(
                                f"Worker {worker_id} started processing task: {task.task_name}"
                            )
                            start_time = time.time()
                            self.process_task(task)
                            end_time = time.time()
                            logger.info(
                                f"Worker {worker_id} finished task: {task.task_name} "
                                f"in {end_time - start_time:.2f} seconds"
                            )
                        except TaskProcessingError as e:
                            logger.error(
                                f"Worker {worker_id} encountered a TaskProcessingError "
                                f"while processing task '{task.task_name}': {e}"
                            )
                            # Re-enqueue if retries remain
                            if task.payload.get("retries", 0) > 0:
                                task.payload["retries"] -= 1
                                self.enqueue_task(task.to_dict(), payload=task.payload)
                        except Exception as e:
                            logger.error(
                                f"Worker {worker_id} encountered an unexpected error "
                                f"while processing task '{task.task_name}': {e}"
                            )
                            # Re-enqueue if retries remain
                            if task.payload.get("retries", 0) > 0:
                                task.payload["retries"] -= 1
                                self.enqueue_task(task.to_dict(), payload=task.payload)
                    else:
                        # This server can't process that task - requeue it
                        with self.redis_client.pipeline() as pipe:
                            pipe.rpush("ml_tasks", task_json)
                            pipe.sadd("queued_tasks", task.task_id)
                            pipe.execute()

                except Exception as e:
                    logger.error(
                        f"Worker {worker_id} crashed with error: {e}. Restarting worker..."
                    )

        for i in range(no_of_workers):
            worker_thread = threading.Thread(target=worker_loop, args=(i,))
            worker_thread.daemon = True
            worker_thread.start()
            self.worker_threads.append(worker_thread)

        task_names = ", ".join(self.allowed_tasks) if self.allowed_tasks else "No tasks registered"
        logger.info(
            f"ModelQ worker started successfully with {no_of_workers} worker(s). "
            f"Connected to Redis at {self.redis_client.connection_pool.connection_kwargs['host']}:"
            f"{self.redis_client.connection_pool.connection_kwargs['port']}. "
            f"Registered tasks: {task_names}"
        )

    def check_middleware(self, middleware_event: str):
        """
        Hook to run middleware if defined.
        """
        logger.info(f"Middleware event triggered: {middleware_event}")
        if self.middleware:
            self.middleware.execute(event=middleware_event)

    def process_task(self, task: Task) -> None:
        """
        Main function to process a task. If 'timeout' is provided, we run
        the function in a separate thread with a join-based timeout.
        WARNING: If the function is CPU-bound and does not cooperate,
        it will not truly be killed when the timeout is reached.
        """
        if task.task_name not in self.allowed_tasks:
            task.status = "failed"
            task.result = "Task not allowed"
            self._save_task_result(task)
            logger.error(f"Task {task.task_name} is not allowed")
            raise TaskProcessingError(f"Task not allowed: {task.task_name}")

        # Retrieve the actual function
        task_function = getattr(self, task.task_name, None)
        if not task_function:
            task.status = "failed"
            task.result = "Task function not found"
            self._save_task_result(task)
            logger.error(f"Task {task.task_name} failed because the task function was not found")
            raise TaskProcessingError(f"Task function not found: {task.task_name}")

        try:
            logger.info(
                f"Processing task: {task.task_name} with args: {task.payload.get('args', [])} "
                f"and kwargs: {task.payload.get('kwargs', {})}"
            )
            start_time = time.time()
            timeout = task.payload.get("timeout", None)
            stream = task.payload.get("stream", False)

            # Indicate processing status in Redis
            self._set_task_status(task.task_id, "processing")

            if stream:
                # For a generator function, yield partial results
                for result in task_function(*task.payload.get("args", []),
                                            **task.payload.get("kwargs", {})):
                    task.status = "in_progress"
                    # Push partial result to a Redis stream
                    self.redis_client.xadd(
                        f"task_stream:{task.task_id}",
                        {"result": json.dumps(result)}
                    )
                task.status = "completed"
                self._save_task_result(task)
            else:
                # Run with or without thread-based timeout
                if timeout:
                    result = self._run_with_timeout(
                        task_function,
                        timeout,
                        *task.payload.get("args", []),
                        **task.payload.get("kwargs", {})
                    )
                else:
                    result = task_function(
                        *task.payload.get("args", []),
                        **task.payload.get("kwargs", {})
                    )

                result_str = task._convert_to_string(result)
                task.result = result_str
                task.status = "completed"
                self._save_task_result(task)

            end_time = time.time()
            logger.info(
                f"Task {task.task_name} completed successfully "
                f"in {end_time - start_time:.2f} seconds"
            )

            # Update main task object in Redis
            self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))

        except TaskTimeoutError as te:
            # Timeout specifically
            self.check_middleware("on_timeout")
            task.status = "failed"
            task.result = f"Timeout: {str(te)}"
            self._save_task_result(task)
            # Publish failure so external code can handle it
            self.redis_client.publish("task_failures", json.dumps({
                "task_id": task.task_id,
                "error": str(te)
            }))
            logger.error(f"Task {task.task_name} timed out: {te}")
            raise TaskProcessingError(f"{task.task_name} timed out: {te}")

        except Exception as e:
            # Generic failure: capture traceback
            tb_str = traceback.format_exc()
            full_error_msg = f"Task {task.task_name} failed with error:\n{tb_str}"

            task.status = "failed"
            task.result = full_error_msg
            self._save_task_result(task)

            # Publish failure so external code can handle it
            self.redis_client.publish("task_failures", json.dumps({
                "task_id": task.task_id,
                "error": full_error_msg
            }))
            logger.error(full_error_msg)
            raise TaskProcessingError(full_error_msg)

    def _run_with_timeout(self, func, timeout, *args, **kwargs):
        """
        Thread-based timeout approach. This does NOT forcibly kill a CPU-bound function.
        It starts a thread and waits up to 'timeout' seconds for it to complete.
        If it doesn't complete, we raise TaskTimeoutError.
        """

        result_container = {"value": None}
        error_container = {"exception": None}

        def target():
            try:
                result_container["value"] = func(*args, **kwargs)
            except Exception as ex:
                error_container["exception"] = ex

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            logger.error(f"Task exceeded timeout of {timeout} seconds")
            # The thread is still running, but we can't kill it forcibly.
            raise TaskTimeoutError(f"Task exceeded timeout of {timeout} seconds")

        # If the function raised an exception inside the thread
        if error_container["exception"]:
            raise error_container["exception"]

        return result_container["value"]

    def get_task_status(self, task_id: str) -> Optional[str]:
        """
        Return the status of a given task_id from Redis.
        """
        task_data = self.redis_client.get(f"task:{task_id}")
        if task_data:
            task_dict = json.loads(task_data)
            return task_dict.get("status")
        return None

    def _set_task_status(self, task_id: str, status: str):
        """
        Utility to update the 'status' of a task in Redis.
        """
        task_data = self.redis_client.get(f"task:{task_id}")
        if not task_data:
            return
        task_dict = json.loads(task_data)
        task_dict["status"] = status
        self.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

    def _save_task_result(self, task: Task):
        """
        Save the task object in 'task_result' and the main 'task:ID' key in Redis.
        Also sets a TTL of 1 hour on 'task_result' if you want some data expiration.
        """
        self.redis_client.set(
            f"task_result:{task.task_id}",
            json.dumps(task.to_dict()),
            ex=3600,  # optional expiration
        )
        self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
