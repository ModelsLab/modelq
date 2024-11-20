from typing import Optional, Dict, Any, Generator
import redis
import json
import functools
import threading
import time
import sqlite3
from celery_ml.app.tasks import Task
from celery_ml.exceptions import TaskProcessingError, TaskTimeoutError
from celery_ml.app.cache import Cache
from celery_ml.app.middleware import Middleware

class CeleryML:
    """CeleryML class for managing machine learning tasks with Redis queueing and streaming."""

    def __init__(
            self,
            host: str = "localhost",
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
        self.allowed_tasks = set()
        self.cache_db_path = cache_db_path
        self.cache = Cache(db_path=cache_db_path)
        self.task_configurations: Dict[str, Dict[str, Any]] = {}
        self.middleware: Middleware = None

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

    def enqueue_task(self, task_name: str, payload: dict):
        task = {
            **task_name,
            "status": "queued"
        }

        self.redis_client.rpush("ml_tasks", json.dumps(task))

    def task(self, task_class=Task, timeout: Optional[int] = None, stream: bool = False):
        """Decorator to create a task. Allows specifying a custom task class, timeout, and streaming support."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                task_name = func.__name__
                payload = {
                    "args": args,
                    "kwargs": kwargs,
                    "timeout": timeout,
                    "stream": stream
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
            return wrapper
        return decorator

    def start_worker(self):
        self.check_middleware("before_worker_boot")
        def worker_loop():
            while True:
                task_data = self.redis_client.blpop("ml_tasks")
                if task_data:
                    _, task_json = task_data
                    task_dict = json.loads(task_json)
                    print(task_dict)
                    task = Task.from_dict(task_dict)
                    print(task)
                    try:
                        self.process_task(task)
                    except TaskProcessingError as e:
                        print(f"Error processing task: {e}")
        worker_thread = threading.Thread(target=worker_loop)
        worker_thread.daemon = True
        worker_thread.start()

    def check_middleware(self, middleware_event: str):
        print(f"got {middleware_event}")
        if self.middleware:
            self.middleware.execute(event=middleware_event)

    def process_task(self, task: Task) -> None:
        """Processes a given task."""
        if task.task_name in self.allowed_tasks:
            task_function = getattr(self, task.task_name, None)
            if task_function:
                try:
                    print(f"Processing task: {task.task_name} with args: {task.payload.get('args', [])} and kwargs: {task.payload.get('kwargs', {})}")
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
                        task.result = result
                        task.status = "completed"
                        self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    # Store updated task status in cache
                    task.store_in_cache(self.cache_db_path)
                    print(f"Task {task.task_name} completed successfully")
                except Exception as e:
                    task.status = "failed"
                    task.result = str(e)
                    self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    # Store failed task status in cache
                    task.store_in_cache(self.cache_db_path)
                    raise TaskProcessingError(task.task_name, str(e))
            else:
                task.status = "failed"
                task.result = "Task function not found"
                self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                # Store failed task status in cache
                task.store_in_cache(self.cache_db_path)
                raise TaskProcessingError(task.task_name, "Task function not found")
        else:
            task.status = "failed"
            task.result = "Task not allowed"
            self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
            # Store failed task status in cache
            task.store_in_cache(self.cache_db_path)
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
