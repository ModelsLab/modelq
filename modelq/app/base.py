import redis
import json
import functools
import threading
import time
import uuid
import logging
from typing import Optional, Dict, Any
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
        self.register_server()
        # Instead of requeueing immediately, consider making it optional or asynchronous
        # self.requeue_cached_tasks()

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
        # task_name is actually a dictionary (from task.to_dict()), ensure correct usage
        # Convert the combined dict
        task = {**task_name, "status": "queued"}
        task_id = task.get("task_id")

        if not self._is_task_in_queue(task_id):
            # Use a pipeline to reduce overhead
            with self.redis_client.pipeline() as pipe:
                pipe.rpush("ml_tasks", json.dumps(task))
                pipe.sadd("queued_tasks", task_id)  # track queued tasks in a set
                pipe.execute()
        else:
            logger.warning(f"Task {task_id} is already in the queue, skipping enqueue.")

    def _is_task_in_queue(self, task_id: str) -> bool:
        # O(1) check using a set instead of scanning the entire queue
        return self.redis_client.sismember("queued_tasks", task_id)

    def requeue_cached_tasks(self):
        # Previously, this would scan all keys. Now we assume all queued tasks are known.
        # If you must scan keys, consider using SCAN or having a separate set of cached tasks.
        queued_task_ids = self.redis_client.smembers("queued_tasks")
        # For each queued task id, ensure it is actually in the ml_tasks list.
        # If not, re-push it.
        # This step might not be needed if your data structures are consistent.
        # If you do need it, do a quick membership check or verification here.
        
        # Example: If tasks might drop out of the 'ml_tasks' list due to a crash:
        # (Not strictly necessary if data is consistent)
        # NOTE: This can become a no-op if your system is always consistent.
        pass

    def get_all_queued_tasks(self) -> list:
        # Instead of scanning keys, we directly use 'queued_tasks' set.
        # This returns only IDs, so we need to fetch their data from Redis.
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
                # Enqueue the task and store in Redis
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
                    # Using BLPOP blocks until a task is available
                    task_data = self.redis_client.blpop("ml_tasks")
                    if task_data:
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
                                    f"Worker {worker_id} finished task: {task.task_name} in {end_time - start_time:.2f} seconds"
                                )
                            except TaskProcessingError as e:
                                logger.error(
                                    f"Worker {worker_id} encountered a TaskProcessingError while processing task '{task.task_name}': {e}"
                                )
                                if task.payload.get("retries", 0) > 0:
                                    task.payload["retries"] -= 1
                                    self.enqueue_task(task.to_dict(), payload=task.payload)
                            except Exception as e:
                                logger.error(
                                    f"Worker {worker_id} encountered an unexpected error while processing task '{task.task_name}': {e}"
                                )
                                if task.payload.get("retries", 0) > 0:
                                    task.payload["retries"] -= 1
                                    self.enqueue_task(task.to_dict(), payload=task.payload)
                        else:
                            # Requeue the task if this server cannot process it
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
            f"{self.redis_client.connection_pool.connection_kwargs['port']}. Registered tasks: {task_names}"
        )

    def check_middleware(self, middleware_event: str):
        logger.info(f"Middleware event triggered: {middleware_event}")
        if self.middleware:
            self.middleware.execute(event=middleware_event)

    def process_task(self, task: Task) -> None:
        if task.task_name in self.allowed_tasks:
            task_function = getattr(self, task.task_name, None)
            if task_function:
                try:
                    logger.info(
                        f"Processing task: {task.task_name} with args: {task.payload.get('args', [])} and kwargs: {task.payload.get('kwargs', {})}"
                    )
                    start_time = time.time()
                    timeout = task.payload.get("timeout", None)
                    stream = task.payload.get("stream", False)
                    if stream:
                        for result in task_function(*task.payload.get("args", []), **task.payload.get("kwargs", {})):
                            task.status = "in_progress"
                            self.redis_client.xadd(f"task_stream:{task.task_id}", {"result": json.dumps(result)})
                        task.status = "completed"
                        self.redis_client.set(
                            f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600
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
                        self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    end_time = time.time()
                    logger.info(f"Task {task.task_name} completed successfully in {end_time - start_time:.2f} seconds")
                    self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                except Exception as e:
                    task.status = "failed"
                    task.result = str(e)
                    self.redis_client.set(
                        f"task_result:{task.task_id}",
                        json.dumps(task.to_dict()),
                        ex=3600,
                    )
                    self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                    logger.error(f"Task {task.task_name} failed with error: {e}")
                    raise TaskProcessingError(task.task_name, str(e))
            else:
                task.status = "failed"
                task.result = "Task function not found"
                self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
                logger.error(f"Task {task.task_name} failed because the task function was not found")
                raise TaskProcessingError(task.task_name, "Task function not found")
        else:
            task.status = "failed"
            task.result = "Task not allowed"
            self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
            self.redis_client.set(f"task:{task.task_id}", json.dumps(task.to_dict()))
            logger.error(f"Task {task.task_name} is not allowed")
            raise TaskProcessingError(task.task_name, "Task not allowed")

    def _run_with_timeout(self, func, timeout, *args, **kwargs):
        result = [None]
        exception = [None]

        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                exception[0] = e

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
