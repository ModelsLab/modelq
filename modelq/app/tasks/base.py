import uuid
import time
import json
import redis
import base64
from typing import Any, Dict, Optional, Generator
from modelq.exceptions import TaskTimeoutError, TaskProcessingError
from PIL import Image, PngImagePlugin
import io
import copy
from typing import Type

class Task:
    def __init__(self, task_name: str, payload: dict, timeout: int = 15, task_id: Optional[str] = None, additional_params: Optional[Dict[str, Any]] = None):
        self.task_id = task_id if task_id else str(uuid.uuid4())
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

        # Additional parameters that will be included in task response
        self.additional_params = additional_params or {}

    def to_dict(self):
        base_dict = {
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
        # Add additional_params to the base response if they exist
        if self.additional_params:
            base_dict.update(self.additional_params)
        return base_dict

    @staticmethod
    def from_dict(data: dict) -> "Task":
        # Extract additional_params from data (any keys not in the standard set)
        standard_keys = {
            "task_id", "task_name", "payload", "status", "result",
            "created_at", "queued_at", "started_at", "finished_at", "stream"
        }
        additional_params = {k: v for k, v in data.items() if k not in standard_keys}

        task = Task(
            task_name=data["task_name"],
            payload=data["payload"],
            additional_params=additional_params if additional_params else None
        )
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

    def get_stream(
        self,
        redis_client: redis.Redis,
        timeout: int = 300
    ) -> Generator[Any, None, None]:
        """
        Generator to yield results from a streaming task.
        Continuously reads from a Redis stream and stops when
        the task is completed, failed, cancelled, or timeout is reached.

        Args:
            redis_client: Redis client instance
            timeout: Maximum time to wait for stream in seconds (default 300s/5min)
        """
        stream_key = f"task_stream:{self.task_id}"
        last_id = "0"
        completed = False
        start_time = time.time()

        while not completed:
            # Check timeout
            if time.time() - start_time > timeout:
                raise TaskTimeoutError(self.task_id)

            # Check if task was cancelled
            if redis_client.get(f"task:{self.task_id}:cancelled"):
                self.status = "cancelled"
                return

            # block=1000 => block for up to 1s, count=10 => max 10 messages
            results = redis_client.xread({stream_key: last_id}, block=1000, count=10)
            if results:
                for _, messages in results:
                    for message_id, message_data in messages:
                        result = json.loads(message_data[b"result"].decode("utf-8"))
                        yield result
                        last_id = message_id
                        # Append to combined_result (handle non-string types)
                        if isinstance(result, str):
                            self.combined_result += result
                        else:
                            self.combined_result += json.dumps(result)

            # Check if the task is finished or failed
            task_json = redis_client.get(f"task_result:{self.task_id}")
            if task_json:
                task_data = json.loads(task_json)
                status = task_data.get("status")
                if status == "completed":
                    completed = True
                    # Update local fields
                    self.status = "completed"
                    self.result = self.combined_result
                elif status == "failed":
                    error_message = task_data.get("result", "Task failed without an error message")
                    raise TaskProcessingError(
                        task_data.get("task_name", self.task_name),
                        error_message
                    )
                elif status == "cancelled":
                    self.status = "cancelled"
                    return

        return

    def get_result(
        self,
        redis_client: redis.Redis,
        timeout: int = None,
        returns: Optional[Type[Any]] = None,
        modelq_ref: Any = None,
    ) -> Any:
        """
        Waits for the result of the task until the timeout.
        Raises TaskProcessingError if the task failed or was cancelled,
        or TaskTimeoutError if it never completes within the timeout.
        Optionally validates/deserializes the result using a Pydantic model.
        """
        if not timeout:
            timeout = self.timeout

        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if task was cancelled
            if redis_client.get(f"task:{self.task_id}:cancelled"):
                self.status = "cancelled"
                raise TaskProcessingError(self.task_name, "Task was cancelled")

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
                elif self.status == "cancelled":
                    raise TaskProcessingError(self.task_name, "Task was cancelled")
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

    def get_progress(self, redis_client: redis.Redis) -> Optional[Dict[str, Any]]:
        """
        Get the current progress of this task.

        Returns dict with:
            - progress: float between 0.0 and 1.0
            - message: optional progress message
            - updated_at: timestamp of last update
        Returns None if no progress has been reported.
        """
        progress_data = redis_client.get(f"task:{self.task_id}:progress")
        if progress_data:
            return json.loads(progress_data)
        return None

    def is_cancelled(self, redis_client: redis.Redis) -> bool:
        """
        Check if this task has been cancelled.
        """
        cancelled = redis_client.get(f"task:{self.task_id}:cancelled")
        return cancelled is not None
