from typing import Optional, Dict, Any, Generator
import redis
import json
import functools
import threading
import time
import sqlite3
import uuid
import logging
from modelq.app.tasks import Task
from modelq.exceptions import TaskProcessingError, TaskTimeoutError
from modelq.app.cache import Cache
from modelq.app.middleware import Middleware

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ModelQ:
    """ModelQ class for managing machine learning tasks with Redis queueing and streaming."""

    def __init__(
            self,
            host: str = "localhost",
            server_id: Optional[str] = None,
            username: str = None,
            port: str = 6379,
            db: int = 0,
            password: str = None,
            ssl: bool = False,
            ssl_cert_reqs: any = None,
            cache_db_path: str = "cache.db",
            **kwargs
    ):
        self.redis_client = self._connect_to_redis(
            host=host,
            port=port,
            db=db,
            password=password,
            username=username,
            ssl=ssl,
            ssl_cert_reqs=ssl_cert_reqs,
            **kwargs
        )
        self.server_id = server_id or str(uuid.uuid4())
        self.allowed_tasks = set()
        self.cache_db_path = cache_db_path
        self.cache = Cache(db_path=cache_db_path)
        self.task_configurations: Dict[str, Dict[str, Any]] = {}
        self.middleware: Middleware = None
        self.register_server()
        self.worker_threads = []

    def _connect_to_redis(
            self, host: str, port: str, db: int, password: str, ssl: bool, ssl_cert_reqs: any, username: str
    ) -> redis.Redis:
        if host == "localhost":
            connection = redis.Redis(host="localhost", db=3)
        else:
            connection = redis.Redis(
                host=host,
                port=port,
                password=password,
                username=username,
                ssl=ssl,
                ssl_cert_reqs=ssl
            )

        return connection

    def register_server(self):
        """Registers the server in Redis with its capabilities."""
        self.redis_client.hset("servers", self.server_id, json.dumps({"allowed_tasks": list(self.allowed_tasks), "status": "idle"}))

    def update_server_status(self, status: str):
        """Updates the server status in Redis."""
        server_data = json.loads(self.redis_client.hget("servers", self.server_id))
        server_data["status"] = status
        self.redis_client.hset("servers", self.server_id, json.dumps(server_data))

    def enqueue_task(self, task_name: str, payload: dict):
        task = {
            **task_name,
            "status": "queued"
        }

        self.redis_client.rpush("ml_tasks", json.dumps(task))

    def task(self, task_class=Task, timeout: Optional[int] = None, stream: bool = False, retries: int = 0):
        """Decorator to create a task. Allows specifying a custom task class, timeout, streaming support, and retries."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                task_name = func.__name__
                payload = {
                    "args": args,
                    "kwargs": kwargs,
                    "timeout": timeout,
                    "stream": stream,
                    "retries": retries
                }
                task = task_class(task_name=task_name, payload=payload)
                if stream:
                    task.stream = True
                self.enqueue_task(task.to_dict(), payload=payload)
                # Store task in cache
                task.store_in_cache(self.cache_db_path)
                return task
            # Attach the function to the instance so it can be called by process_task
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
                    # Update server status to idle while waiting for tasks
                    self.update_server_status(f"worker_{worker_id}: idle")
                    task_data = self.redis_client.blpop("ml_tasks")
                    if task_data:
                        # Update server status to busy when a task is picked up
                        self.update_server_status(f"worker_{worker_id}: busy")
                        _, task_json = task_data
                        task_dict = json.loads(task_json)
                        task = Task.from_dict(task_dict)
                        if task.task_name in self.allowed_tasks:
                            try:
                                logger.info(f"Worker {worker_id} started processing task: {task.task_name}")
                                start_time = time.time()
                                self.process_task(task)
                                end_time = time.time()
                                logger.info(f"Worker {worker_id} finished task: {task.task_name} in {end_time - start_time:.2f} seconds")
                            except TaskProcessingError as e:
                                logger.error(f"Worker {worker_id} encountered a TaskProcessingError while processing task '{task.task_name}': {e}")
                                if task.payload.get("retries", 0) > 0:
                                    task.payload["retries"] -= 1
                                    self.enqueue_task(task.to_dict(), payload=task.payload)
                            except Exception as e:
                                logger.error(f"Worker {worker_id} encountered an unexpected error while processing task '{task.task_name}': {e}")
                                if task.payload.get("retries", 0) > 0:
                                    task.payload["retries"] -= 1
                                    self.enqueue_task(task.to_dict(), payload=task.payload)
                        else:
                            # Requeue the task if this server cannot process it
                            self.redis_client.rpush("ml_tasks", task_json)
                except Exception as e:
                    logger.error(f"Worker {worker_id} crashed with error: {e}. Restarting worker...")
        
        for i in range(no_of_workers):
            worker_thread = threading.Thread(target=worker_loop, args=(i,))
            worker_thread.daemon = True
            worker_thread.start()
            self.worker_threads.append(worker_thread)

        # Log after all workers have started
        task_names = ', '.join(self.allowed_tasks) if self.allowed_tasks else 'No tasks registered'
        logger.info(f"ModelQ worker started successfully with {no_of_workers} worker(s). Connected to Redis at {self.redis_client.connection_pool.connection_kwargs['host']}:{self.redis_client.connection_pool.connection_kwargs['port']}. Registered tasks: {task_names}")

    def check_middleware(self, middleware_event: str):
        logger.info(f"Middleware event triggered: {middleware_event}")
        if self.middleware:
            self.middleware.execute(event=middleware_event)

    def process_task(self, task: Task) -> None:
        """Processes a given task."""
        if task.task_name in self.allowed_tasks:
            task_function = getattr(self, task.task_name, None)
            if task_function:
                try:
                    logger.info(f"Processing task: {task.task_name} with args: {task.payload.get('args', [])} and kwargs: {task.payload.get('kwargs', {})}")
                    start_time = time.time()
                    timeout = task.payload.get("timeout", None)
                    if task.payload.get("stream", False):
                        for result in task_function(*task.payload.get("args", []), **task.payload.get("kwargs", {})):
                            task.status = "in_progress"
                            self.redis_client.xadd(f"task_stream:{task.task_id}", {"result": json.dumps(result)})
                        # Mark the task as completed when streaming ends
                        task.status = "completed"
                        self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    else:
                        if timeout:
                            result = self._run_with_timeout(task_function, timeout, *task.payload.get("args", []), **task.payload.get("kwargs", {}))
                        else:
                            result = task_function(*task.payload.get("args", []), **task.payload.get("kwargs", {}))
                        result_str = task._convert_to_string(result)
                        task.result = result_str
                        task.status = "completed"
                        self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    end_time = time.time()
                    logger.info(f"Task {task.task_name} completed successfully in {end_time - start_time:.2f} seconds")
                    # Store updated task status in cache
                    task.store_in_cache(self.cache_db_path)
                except Exception as e:
                    task.status = "failed"
                    task.result = str(e)
                    self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    # Store failed task status in cache
                    task.store_in_cache(self.cache_db_path)
                    logger.error(f"Task {task.task_name} failed with error: {e}")
                    raise TaskProcessingError(task.task_name, str(e))
            else:
                task.status = "failed"
                task.result = "Task function not found"
                self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                # Store failed task status in cache
                task.store_in_cache(self.cache_db_path)
                logger.error(f"Task {task.task_name} failed because the task function was not found")
                raise TaskProcessingError(task.task_name, "Task function not found")
        else:
            task.status = "failed"
            task.result = "Task not allowed"
            self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
            # Store failed task status in cache
            task.store_in_cache(self.cache_db_path)
            logger.error(f"Task {task.task_name} is not allowed")
            raise TaskProcessingError(task.task_name, "Task not allowed")

    def _run_with_timeout(self, func, timeout, *args, **kwargs):
        """Runs a function with a timeout."""
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

    def get_all_queued_tasks(self) -> list:
        """Retrieves all tasks with status 'queued' from the SQLite cache database."""
        with sqlite3.connect(self.cache_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT task_id, task_name, status FROM tasks WHERE status = ?', ("queued",))
            rows = cursor.fetchall()
            return [{"task_id": row[0], "task_name": row[1], "status": row[2]} for row in rows]

    def get_task_status(self, task_id: str) -> Optional[str]:
        """Retrieves the status of a particular task by task ID from the SQLite cache database."""
        with sqlite3.connect(self.cache_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT status FROM tasks WHERE task_id = ?', (task_id,))
            row = cursor.fetchone()
            if row:
                return row[0]
            return None
