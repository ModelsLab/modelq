import uuid
import time
import json
import redis
import base64
from typing import Any, Optional, Generator
from PIL import Image, PngImagePlugin
import io

from modelq.exceptions import TaskTimeoutError, TaskProcessingError


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

    def to_dict(self) -> dict:
        """
        Convert task object to a dictionary for JSON serialization.
        """
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "payload": self.payload,
            "status": self.status,
            "result": self.result,
            "timestamp": self.timestamp,
            "stream": self.stream,
        }

    @staticmethod
    def from_dict(data: dict) -> "Task":
        """
        Create a Task object from a dictionary.
        """
        task = Task(task_name=data["task_name"], payload=data["payload"])
        task.task_id = data["task_id"]
        task.status = data["status"]
        task.result = data.get("result")
        task.timestamp = data["timestamp"]
        task.stream = data.get("stream", False)
        return task

    def _convert_to_string(self, data: Any) -> str:
        """
        Converts various data types (JSON-serializable or PIL images) to a
        string/base64 representation.
        """
        try:
            # If data is JSON-serializable, convert with json.dumps
            if isinstance(data, (dict, list, int, float, bool)):
                return json.dumps(data)
            # If data is a Pillow image, convert it to base64 PNG
            elif isinstance(data, (Image.Image, PngImagePlugin.PngImageFile)):
                buffered = io.BytesIO()
                data.save(buffered, format="PNG")
                return "data:image/png;base64," + base64.b64encode(
                    buffered.getvalue()
                ).decode("utf-8")
            # Otherwise, just convert to string
            return str(data)
        except TypeError:
            return str(data)

    def get_result(
        self,
        redis_client: redis.Redis,
        timeout: Optional[int] = None,
        sleep_interval: float = 0.2,
    ) -> Any:
        """
        Waits for the result of the task until 'timeout' (or self.timeout if None).
        Polls Redis every 'sleep_interval' seconds (default 0.2) to reduce latency.

        - If the task is 'completed', returns its result immediately.
        - If the task is 'failed', raises TaskProcessingError with the error message.
        - If the timeout is exceeded, raises TaskTimeoutError.
        """
        if timeout is None:
            timeout = self.timeout

        start_time = time.time()

        while (time.time() - start_time) < timeout:
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                self.result = task_data.get("result")
                self.status = task_data.get("status")

                # If completed, return the result
                if self.status == "completed":
                    return self.result

                # If failed, raise an error with the stored message
                if self.status == "failed":
                    error_msg = self.result or "Task failed without details"
                    raise TaskProcessingError(f"Task {self.task_id} failed: {error_msg}")

            time.sleep(sleep_interval)

        # If we get here, the timeout was reached without completion
        raise TaskTimeoutError(
            f"Task {self.task_id} did not complete within {timeout} seconds."
        )

    def get_stream(self, redis_client: redis.Redis) -> Generator[Any, None, None]:
        """
        Generator to yield results from a streaming task. It uses XREAD with
        a 1-second block interval. If you want faster streaming updates, reduce
        the block interval or add extra logic.

        - Yields partial results as they appear in the Redis stream.
        - If the task is marked 'completed' in Redis, stops reading.
        - If the task is marked 'failed', raises TaskProcessingError immediately.
        """
        stream_key = f"task_stream:{self.task_id}"
        last_id = "0"
        completed = False

        while not completed:
            # Block for up to 1s to read any new stream entries
            results = redis_client.xread({stream_key: last_id}, block=1000, count=10)
            if results:
                for _, messages in results:
                    for message_id, message_data in messages:
                        result_str = message_data[b"result"].decode("utf-8")
                        result = json.loads(result_str)
                        yield result
                        last_id = message_id
                        # Concatenate result for final combined_result
                        self.combined_result += result

            # After reading messages, check if task is completed/failed
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                if task_data.get("status") == "completed":
                    completed = True
                    self.status = "completed"
                    self.result = self.combined_result

                elif task_data.get("status") == "failed":
                    self.status = "failed"
                    self.result = task_data.get("result")
                    error_msg = self.result or "Task failed without details"
                    raise TaskProcessingError(f"Task {self.task_id} failed: {error_msg}")

        return
