from typing import Optional , Dict , Any
import redis
import json
import functools
from celery_ml.app.tasks import Task
from celery_ml.exceptions import TaskProcessingError , TaskTimeoutError
from celery_ml.app.cache import Cache
import threading

class CeleryML :
    """"""

    def __init__(
            self,
            host : str = "localhost",
            username : str = None,
            port : str = 6379,
            db : int = 0  ,
            password : str = None,
            ssl : bool = False,
            ssl_cert_reqs : any = None,
            **kwargs
                 ):
        self.redis_client = self._connect_to_redis(
            host=host,
            port = port,
            db = db,
            password= password,
            username = username,
            ssl = ssl,
            ssl_cert_reqs= ssl_cert_reqs,
            **kwargs
        )
        self.allowed_tasks = set()
        self.cache = Cache()
        self.task_configurations: Dict[str, Dict[str, Any]] = {}
    
    def _connect_to_redis(
            self,host : str,port : str , db : int ,  password : str,ssl : bool , ssl_cert_reqs : any,username : str
    ) -> redis.Redis:
        if host == "localhost":
            connection = redis.Redis(host = "localhost",db = 3) 

        else :
            connection = redis.Redis(
                host=host,
                port=port,
                password=password,
                username=username,
                ssl=ssl,
                ssl_cert_reqs=ssl
            )

        return connection
    
    def enqueue_task(self,task_name : str , payload : dict) :
        task = {
            **task_name,
            "status" : "queued"
        }

        self.redis_client.rpush("ml_tasks",json.dumps(task))

    def task(self, task_class=Task):
        """Decorator to create a task. Allows specifying a custom task class."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                task_name = func.__name__
                payload = {
                    "args": args,
                    "kwargs": kwargs
                }
                task = task_class(task_name=task_name, payload=payload)
                self.enqueue_task(task.to_dict(),payload=payload)
                return task.get_result(self.redis_client)
            # Attach the function to the instance so it can be called by process_task
            setattr(self, func.__name__, func)
            self.allowed_tasks.add(func.__name__)
            return wrapper
        return decorator
    
    def start_worker(self):
        def worker_loop():
            while True :
                task_data = self.redis_client.blpop("ml_tasks")
                if task_data :
                    _, task_json = task_data
                    task_dict = json.loads(task_json)
                    print(task_dict)
                    task = Task.from_dict(task_dict)
                    print(task)
                    try :
                        self.process_task(task)
                    except TaskProcessingError as e :
                        print(f"Error processing task : {e}")
        worker_thread = threading.Thread(target=worker_loop)
        worker_thread.daemon = True
        worker_thread.start()

    def process_task(self, task: Task) -> None:
        """Processes a given task."""
        if task.task_name in self.allowed_tasks:
            task_function = getattr(self, task.task_name, None)
            if task_function:
                try:
                    print(f"Processing task: {task.task_name} with args: {task.payload.get('args', [])} and kwargs: {task.payload.get('kwargs', {})}")
                    result = task_function(*task.payload.get("args", []), **task.payload.get("kwargs", {}))
                    task.result = result
                    task.status = "completed"
                    self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    print(f"Task {task.task_name} completed successfully with result: {result}")
                except Exception as e:
                    task.status = "failed"
                    task.result = str(e)
                    self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                    raise TaskProcessingError(task.task_name, str(e))
            else:
                task.status = "failed"
                task.result = "Task function not found"
                self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
                raise TaskProcessingError(task.task_name, "Task function not found")
        else:
            task.status = "failed"
            task.result = "Task not allowed"
            self.redis_client.set(f"task_result:{task.task_id}", json.dumps(task.to_dict()), ex=3600)
            raise TaskProcessingError(task.task_name, "Task not allowed")