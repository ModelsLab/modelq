from typing import Optional
import redis
import json
import functools

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
            ssh = ssl,
            ssl_cert_reqs= ssl_cert_reqs,
            **kwargs
        )
    
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
            "task_name" : task_name,
            "payload" : payload,
            "status" : "queued"
        }

        self.redis_client.rpush("ml_tasks",json.dumps(task))

    def task(self,func) :
        """Decorator to create tasks."""
        @functools.wraps(func)
        def wrapper(*args,**kwargs):
            task_name = func.__name__
            payload = {
                "args" : args,
                "kwargs" : kwargs
            }
            self.enqueue_task(task_name, payload)
        return wrapper
    
    def process_task(self,task:dict) -> None :
        task_name = task['task_name']
        payload = task['payload']
        args = payload.get("args",[])
        kwargs = payload.get("kwargs",{})

        task_function = getattr(self,task_name , None)
        if task_function :
            try :
                print(f"Processing task: {task_name} with args: {args} and kwargs: {kwargs}")
                task_function(*args,**kwargs)
                print(f"Task {task_name} completed successfully.")
            except Exception as e : 
                print(f"Error processing task {task_name} : {e}")
        else :
            print(f"Task {task_name} not found.")