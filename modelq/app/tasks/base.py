import uuid
import time
import json
import redis
import base64
from typing import Any, Optional, Generator
from modelq.exceptions import TaskTimeoutError, TaskProcessingError
from PIL import Image, PngImagePlugin
import io
import copy
from typing import Type

class Task:
    def __init__(self, task_name: str, payload: dict, timeout: int = 15):
        self.task_id = str(uuid.uuid4())
        self.task_name = task_name
        self.payload = payload
        self.original_payload = copy.deepcopy(payload)
        self.status = "queued"
        self.result = None
        
        # New timestamps:
        self.created_at = time.time()   # When Task object is instantiated
        self.queued_at = None          # When task is enqueued in Redis
        self.started_at = None         # When worker actually starts it
        self.finished_at = None        # When task finishes (success or fail)

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
            "created_at": self.created_at,
            "queued_at": self.queued_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "stream": self.stream,
        }

    @staticmethod
    def from_dict(data: dict) -> "Task":
        task = Task(task_name=data["task_name"], payload=data["payload"])
        task.task_id = data["task_id"]
        task.status = data["status"]
        task.result = data.get("result")

        # Load timestamps if present
        task.created_at = data.get("created_at")
        task.queued_at = data.get("queued_at")
        task.started_at = data.get("started_at")
        task.finished_at = data.get("finished_at")

        task.stream = data.get("stream", False)
        return task

    def _convert_to_string(self, data: Any) -> str:
        """
        Converts data to a string representation. If the data is a PIL image,
        encode it as a base64 PNG.
        """
        try:
            if isinstance(data, (dict, list, int, float, bool)):
                return json.dumps(data)
            elif isinstance(data, (Image.Image, PngImagePlugin.PngImageFile)):
                buffered = io.BytesIO()
                data.save(buffered, format="PNG")
                return "data:image/png;base64," + base64.b64encode(
                    buffered.getvalue()
                ).decode("utf-8")
            return str(data)
        except TypeError:
            return str(data)

    def get_result(self, redis_client: redis.Redis, timeout: int = None) -> Any:
        """
        Waits for the result of the task until the timeout.
        Raises TaskProcessingError if the task failed,
        or TaskTimeoutError if it never completes within the timeout.
        """
        if not timeout:
            timeout = self.timeout

        start_time = time.time()
        while time.time() - start_time < timeout:
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                self.result = task_data.get("result")
                self.status = task_data.get("status")

                if self.status == "failed":
                    # Raise the original error message as a TaskProcessingError
                    error_message = self.result or "Task failed without an error message"
                    raise TaskProcessingError(
                        task_data.get("task_name", self.task_name),
                        error_message
                    )
                elif self.status == "completed":
                    return self.result
                # If status is something else like 'processing', keep polling

            time.sleep(1)

        # If we exit the loop, we timed out
        raise TaskTimeoutError(self.task_id)

    def get_result(
        self,
        redis_client: redis.Redis,
        timeout: int = None,
        returns: Optional[Type[Any]] = None,
        modelq_ref: Any = None,
    ) -> Any:
        """
        Waits for the result of the task until the timeout.
        Raises TaskProcessingError if the task failed,
        or TaskTimeoutError if it never completes within the timeout.
        Optionally validates/deserializes the result using a Pydantic model.
        """
        if not timeout:
            timeout = self.timeout

        start_time = time.time()
        while time.time() - start_time < timeout:
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                self.result = task_data.get("result")
                self.status = task_data.get("status")

                if self.status == "failed":
                    error_message = self.result or "Task failed without an error message"
                    raise TaskProcessingError(
                        task_data.get("task_name", self.task_name),
                        error_message
                    )
                elif self.status == "completed":
                    raw_result = self.result

                    # Auto-detect returns schema if not given
                    if returns is None and modelq_ref is not None:
                        task_function = getattr(modelq_ref, self.task_name, None)
                        returns = getattr(task_function, "_mq_returns", None)

                    if returns is not None:
                        try:
                            if isinstance(raw_result, str):
                                try:
                                    result_data = json.loads(raw_result)
                                except Exception:
                                    result_data = raw_result
                            else:
                                result_data = raw_result

                            if isinstance(result_data, dict):
                                return returns(**result_data)
                            elif isinstance(result_data, returns):
                                return result_data
                            else:
                                return returns.parse_obj(result_data)
                        except Exception as ve:
                            raise TaskProcessingError(
                                self.task_name,
                                f"Result validation failed: {ve}"
                            )
                    else:
                        return raw_result

            time.sleep(1)

        raise TaskTimeoutError(self.task_id)
