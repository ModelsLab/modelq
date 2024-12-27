import redis
import json
import functools
import threading
import time
import uuid
import logging
import traceback
from typing import Optional, Dict, Any

import requests  # <-- NEW: To send error payloads to a webhook

from modelq.app.tasks import Task
from modelq.exceptions import TaskProcessingError, TaskTimeoutError
from modelq.app.middleware import Middleware

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
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
        max_connections: int = 50,  # Limit max connections to avoid "too many clients"
        webhook_url: Optional[str] = None,  # <-- NEW: optional webhook for error logging
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
        self.server_id = server_id or str(uuid.uuid4())
        self.allowed_tasks = set()
        self.task_configurations: Dict[str, Dict[str, Any]] = {}
        self.middleware: Middleware = None

        # NEW: Store the webhook URL
        self.webhook_url = webhook_url

        self.register_server()
        self.requeue_inprogress_tasks()

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
            # ssl=ssl,
            # ssl_cert_reqs=ssl_cert_reqs,
            max_connections=max_connections,
        )
        return redis.Redis(connection_pool=pool)

    def register_server(self):
        self.redis_client.hset(
            "servers",
            self.server_id,
            json.dumps({"allowed_tasks": list(self.allowed_tasks), "status": "idle"}),
        )

    def update_server_status(self, status: str):
        server_data = json.loads(self.redis_client.hget("servers", self.server_id))
        server_data["status"] = status
        self.redis_client.hset("servers", self.server_id, json.dumps(server_data))

    def enqueue_task(self, task_name: dict, payload: dict):
        task = {**task_name, "status": "queued"}
        task_id = task.get("task_id")

        if not self._is_task_in_queue(task_id):
            with self.redis_client.pipeline() as pipe:
                pipe.rpush("ml_tasks", json.dumps(task))
                pipe.sadd("queued_tasks", task_id)
                pipe.execute()
        else:
            logger.warning(f"Task {task_id} is already in the queue, skipping enqueue.")

    def _is_task_in_queue(self, task_id: str) -> bool:
        return self.redis_client.sismember("queued_tasks", task_id)

    def requeue_cached_tasks(self):
        """
        (Optional) Re-queue tasks that might be in 'queued_tasks' set
        but missing in 'ml_tasks' list. Currently a placeholder.
        """
        pass

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
                self.redis_client.rpush("ml_tasks", json.dumps(task_dict))
                self.redis_client.sadd("queued_tasks", task_id)
                self.redis_client.srem("processing_tasks", task_id)

    def get_all_queued_tasks(self) -> list:
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
        task_status = self.get_task_status(task_id)
        return task_status in ["processing", "completed"]

    def task(
        self,
        task_class=Task,
        timeout: Optional[int] = None,
        stream: bool = False,
        retries: int = 0,
    ):
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
                self.enqueue_task(task.to_dict(), payload=payload)
                self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                return task

            setattr(self, func.__name__, func)
            self.allowed_tasks.add(func.__name__)
            self.register_server()
            return wrapper
        return decorator

    def start_workers(self, no_of_workers: int = 1):
        if any(thread.is_alive() for thread in self.worker_threads):
            return  # Workers are already running

        self.check_middleware("before_worker_boot")

        def worker_loop(worker_id):
            while True:
                try:
                    self.update_server_status(f"worker_{worker_id}: idle")
                    task_data = self.redis_client.blpop("ml_tasks")
                    if task_data:
                        self.update_server_status(f"worker_{worker_id}: busy")
                        _, task_json = task_data
                        task_dict = json.loads(task_json)
                        task = Task.from_dict(task_dict)

                        self.redis_client.srem("queued_tasks", task.task_id)
                        self.redis_client.sadd("processing_tasks", task.task_id)

                        task.status = "processing"
                        self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))

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
                                    f"Worker {worker_id} encountered a TaskProcessingError: {e}"
                                )
                                if task.payload.get("retries", 0) > 0:
                                    task.payload["retries"] -= 1
                                    self.enqueue_task(task.to_dict(), payload=task.payload)
                            except Exception as e:
                                logger.error(
                                    f"Worker {worker_id} encountered an unexpected error: {e}"
                                )
                                if task.payload.get("retries", 0) > 0:
                                    task.payload["retries"] -= 1
                                    self.enqueue_task(task.to_dict(), payload=task.payload)
                        else:
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
        logger.info(f"Middleware event triggered: {middleware_event}")
        if self.middleware:
            self.middleware.execute(event=middleware_event)

    def log_task_error_to_file(self, task: Task, exc: Exception, file_path="modelq_errors.log"):
        """
        Logs detailed error info to a specified file, with dashes before and after.
        Includes:
          - Task ID
          - Task name
          - Task payload
          - Error message
          - Full traceback
          - Timestamp
        """
        error_trace = traceback.format_exc()  # captures the full traceback
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
        Non-blocking version that constructs the entire Discord message (content),
        then spawns a thread to do the POST request.
        """
        if not self.webhook_url:
            return  # No webhook configured, do nothing
    
        # Capture the full traceback from the current exception context
        full_tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    
        payload_str = json.dumps(task.payload, indent=2)  # pretty-print the payload
    
        content_str = (
            f"**Task Name**: {task.task_name}\n"
            f"**Task ID**: {task.task_id}\n"
            f"**Payload**:\n```json\n{payload_str}\n```\n"
            f"**Error Message**: {exc}\n"
            f"**Traceback**:\n```{full_tb}```"
        )
    
        # Now we have the entire message. Let's pass it to a thread.
        t = threading.Thread(
            target=self._post_error_to_webhook_sync,
            args=(content_str,),
            daemon=True
        )
        t.start()
    
    def _post_error_to_webhook_sync(self, content_str: str):
        """
        Sends the message to Discord in blocking fashion. Called by a background thread.
        """
        payload = {"content": content_str}
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            if resp.status_code >= 400:
                logger.error(
                    f"Failed to POST error to Discord webhook. "
                    f"Status code: {resp.status_code}, Response: {resp.text}"
                )
        except Exception as e2:
            logger.error(f"Exception while sending error to webhook: {e2}")


    def process_task(self, task: Task) -> None:
        """
        Processes the task by calling the registered function.
        """
        try:
            if task.task_name in self.allowed_tasks:
                task_function = getattr(self, task.task_name, None)
                if task_function:
                    logger.info(
                        f"Processing task: {task.task_name} "
                        f"with args: {task.payload.get('args', [])} "
                        f"and kwargs: {task.payload.get('kwargs', {})}"
                    )
                    start_time = time.time()
                    timeout = task.payload.get("timeout", None)
                    stream = task.payload.get("stream", False)

                    if stream:
                        for result in task_function(
                            *task.payload.get("args", []),
                            **task.payload.get("kwargs", {})
                        ):
                            task.status = "in_progress"
                            self.redis_client.xadd(
                                f"task_stream:{task.task_id}",
                                {"result": json.dumps(result)}
                            )
                        task.status = "completed"
                        self.redis_client.set(
                            f"task_result:{task.task_id}",
                            json.dumps(task.to_dict()),
                            ex=3600
                        )
                    else:
                        if timeout:
                            result = self._run_with_timeout(
                                task_function,
                                timeout,
                                *task.payload.get("args", []),
                                **task.payload.get("kwargs", {}),
                            )
                        else:
                            result = task_function(
                                *task.payload.get("args", []),
                                **task.payload.get("kwargs", {}),
                            )
                        result_str = task._convert_to_string(result)
                        task.result = result_str
                        task.status = "completed"
                        self.redis_client.set(
                            f"task_result:{task.task_id}",
                            json.dumps(task.to_dict()),
                            ex=3600
                        )

                    end_time = time.time()
                    logger.info(
                        f"Task {task.task_name} completed successfully "
                        f"in {end_time - start_time:.2f} seconds"
                    )
                    self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                else:
                    task.status = "failed"
                    task.result = "Task function not found"
                    self.redis_client.set(
                        f"task_result:{task.task_id}",
                        json.dumps(task.to_dict()),
                        ex=3600,
                    )
                    self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                    logger.error(
                        f"Task {task.task_name} failed because the task function was not found"
                    )
                    raise TaskProcessingError(task.task_name, "Task function not found")
            else:
                task.status = "failed"
                task.result = "Task not allowed"
                self.redis_client.set(
                    f"task_result:{task.task_id}",
                    json.dumps(task.to_dict()),
                    ex=3600,
                )
                self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                logger.error(f"Task {task.task_name} is not allowed")
                raise TaskProcessingError(task.task_name, "Task not allowed")

        except Exception as e:
            # Mark the task as failed
            task.status = "failed"
            task.result = str(e)
            self.redis_client.set(
                f"task_result:{task.task_id}",
                json.dumps(task.to_dict()),
                ex=3600,
            )
            self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))

            # 1) Log the error to file
            self.log_task_error_to_file(task, e, file_path="modelq_errors.log")

            # 2) POST the error to the webhook (if configured)
            self.post_error_to_webhook(task, e)

            logger.error(f"Task {task.task_name} failed with error: {e}")
            raise TaskProcessingError(task.task_name, str(e))

        finally:
            self.redis_client.srem("processing_tasks", task.task_id)

    def _run_with_timeout(self, func, timeout, *args, **kwargs):
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
            logger.error(f"Task exceeded timeout of {timeout} seconds")
            raise TaskTimeoutError(f"Task exceeded timeout of {timeout} seconds")
        if exception[0]:
            raise exception[0]
        return result[0]

    def get_task_status(self, task_id: str) -> Optional[str]:
        task_data = self.redis_client.get(f"task:{task_id}")
        if task_data:
            task = json.loads(task_data)
            return task.get("status")
        return None
