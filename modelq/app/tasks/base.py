import uuid
import time
import json
import redis
import sqlite3
import base64
from typing import Any, Optional, Generator
from modelq.exceptions import TaskTimeoutError
from PIL import Image, PngImagePlugin
import io


class Task:

    def __init__(self, task_name: str, payload: dict, timeout: int = 15):
        self.task_id = str(uuid.uuid4())
        self.task_name = task_name
        self.payload = payload
        self.status = "queued"
        self.result = None
        self.timestamp = time.time()
        self.timeout = timeout
        self.stream = False
        self.combined_result = ""

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "payload": self.payload,
            "status": self.status,
            "result": self.result,
            "timestamp": self.timestamp,
            "stream": self.stream
        }

    @staticmethod
    def from_dict(data: dict) -> 'Task':
        task = Task(task_name=data["task_name"], payload=data["payload"])
        task.task_id = data["task_id"]
        task.status = data["status"]
        task.result = data.get("result")
        task.timestamp = data["timestamp"]
        task.stream = data.get("stream", False)
        return task

    def _convert_to_string(self, data: Any) -> str:
        """Converts any data type to a string representation, including PIL images."""
        # print(type(data))
        try:
            if isinstance(data, (dict, list, int, float, bool)):
                return json.dumps(data)
            elif isinstance(data, (Image.Image, PngImagePlugin.PngImageFile)):
                print("here")
                buffered = io.BytesIO()
                data.save(buffered, format="PNG")
                return "data:image/png;base64," + base64.b64encode(buffered.getvalue()).decode("utf-8")
            return str(data)
        except TypeError:
            return str(data)

    def store_in_cache(self, db_path: str = "cache.db") -> None:
        """Stores the task in the SQLite cache database."""
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            task_id = self._convert_to_string(self.task_id)
            task_name = self._convert_to_string(self.task_name)
            payload = self._convert_to_string(self.payload)
            status = self._convert_to_string(self.status)
            result = self._convert_to_string(self.result)
            timestamp = self.timestamp if isinstance(self.timestamp, (int, float)) else None

            cursor.execute(
                '''
                INSERT OR REPLACE INTO tasks (task_id, task_name, payload, status, result, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (task_id, task_name, payload, status, result, timestamp)
            )
            conn.commit()

    def get_result(self, redis_client: redis.Redis, timeout: int = None) -> Any:
        """Waits for the result of the task until the timeout."""

        if not timeout:
            timeout = self.timeout

        start_time = time.time()
        while time.time() - start_time < timeout:
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                self.result = task_data.get("result")
                self.status = task_data.get("status")
                # Store the updated task status in cache
                self.store_in_cache()
                return self.result
            time.sleep(1)
        raise TaskTimeoutError(self.task_id)

    def get_stream(self, redis_client: redis.Redis) -> Generator[Any, None, None]:
        """Generator to yield results from a streaming task."""
        stream_key = f"task_stream:{self.task_id}"
        last_id = "0"
        completed = False

        while not completed:
            results = redis_client.xread({stream_key: last_id}, block=1000, count=10)
            if results:
                for _, messages in results:
                    for message_id, message_data in messages:
                        result = json.loads(message_data[b"result"].decode("utf-8"))
                        yield result
                        last_id = message_id
                        # Concatenate result for storing combined response
                        self.combined_result += result
            # Check if the task is marked as completed after yielding current messages
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                if task_data.get("status") == "completed":
                    completed = True
                    # Store the completed task status and combined result in cache
                    self.status = "completed"
                    self.result = self.combined_result
                    self.store_in_cache()
        return
