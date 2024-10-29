import uuid
import time
import json
import redis
from typing import Any , Optional
from celery_ml.exceptions import TaskTimeoutError


class Task :

    def __init__(self,task_name : str , payload : dict,timeout : int = 15):
        self.task_id = str(uuid.uuid4())
        self.task_name = task_name 
        self.payload = payload
        self.status = "queued"
        self.result = None 
        self.timestamp = time.time()
        self.timeout = timeout

    def to_dict(self) :
        return {
            "task_id" : self.task_id,
            "task_name" : self.task_name,
            "payload" : self.payload,
            "status" : self.status,
            "result" : self.result,
            "timestamp" : self.timestamp
        }
    
    @staticmethod
    def from_dict(data : dict ) -> 'Task' :
        task = Task(task = data["task_name"],payload = data["payload"])
        task.task_id = data["task_id"]
        task.status = data["status"]
        task.result = data.get("result")
        task.timestamp = data["timestamp"]
        return task
    

    def get_result(self, redis_client: redis.Redis, timeout: int = None) -> Any:
        """Waits for the result of the task until the timeout."""

        if not timeout :
            timeout = self.timeout

        start_time = time.time()
        while time.time() - start_time < timeout:
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                self.result = task_data.get("result")
                self.status = task_data.get("status")
                return self.result
            time.sleep(1)
        raise TaskTimeoutError(self.task_id)