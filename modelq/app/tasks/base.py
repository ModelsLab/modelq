import uuid
import time
import json
import base64
import io
import copy
import logging
from typing import Any, Optional, Generator, Type

from PIL import Image, PngImagePlugin

from modelq.exceptions import TaskTimeoutError, TaskProcessingError
from modelq.app.backends.redis import RedisQueueBackend  # for stream helper

logger = logging.getLogger("modelq.task")


class Task:
    """Light‑weight DTO representing a single ModelQ job."""

    def __init__(
        self,
        task_name: str,
        payload: dict,
        timeout: int = 15,
        *,
        backend: Optional[Any] = None,
    ):
        self.task_id: str = str(uuid.uuid4())
        self.task_name: str = task_name
        self.payload: dict = payload
        self.original_payload = copy.deepcopy(payload)

        # runtime / bookkeeping
        self.status: str = "queued"
        self.result: Any = None
        self.created_at: float = time.time()
        self.queued_at: Optional[float] = None
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None

        self.timeout: int = timeout
        self.stream: bool = False
        self.combined_result: str = ""

        # store queue backend reference for later use (get_result / get_stream)
        self.backend: Optional[Any] = backend

    # ─────────────────────── Serialization ──────────────────────────
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
    def from_dict(data: dict, *, backend: Optional[Any] = None) -> "Task":
        t = Task(data["task_name"], data["payload"], backend=backend)
        t.task_id = data["task_id"]
        t.status = data.get("status", "queued")
        t.result = data.get("result")
        t.created_at = data.get("created_at")
        t.queued_at = data.get("queued_at")
        t.started_at = data.get("started_at")
        t.finished_at = data.get("finished_at")
        t.stream = data.get("stream", False)
        return t

    # ─────────────────────── Streaming (Redis only for now) ──────────
    def get_stream(self, backend: Any | None = None) -> Generator[Any, None, None]:
        """Yield incremental results for streaming tasks (Redis backend only)."""
        backend = backend or self.backend
        if backend is None:
            raise ValueError("Backend reference required for streaming")
        if not isinstance(backend, RedisQueueBackend):
            raise NotImplementedError("Streaming supported only on Redis backend right now")
        redis_client = backend.redis

        stream_key = f"task_stream:{self.task_id}"
        last_id = "0"
        while True:
            results = redis_client.xread({stream_key: last_id}, block=1000, count=10)
            for _, msgs in results or []:
                for msg_id, data in msgs:
                    payload = json.loads(data[b"result"].decode())
                    self.combined_result += payload if isinstance(payload, str) else json.dumps(payload)
                    yield payload
                    last_id = msg_id
            state = backend.load_task_state(self.task_id)
            if state and state.get("status") in {"completed", "failed"}:
                if state["status"] == "failed":
                    raise TaskProcessingError(self.task_name, state.get("result"))
                self.status = "completed"; self.result = self.combined_result
                break

    # ─────────────────────── Result Retrieval ───────────────────────
    def get_result(
        self,
        backend: Any | None = None,
        timeout: Optional[int] = None,
        returns: Optional[Type[Any]] = None,
        modelq_ref: Any = None,
    ) -> Any:
        backend = backend or self.backend
        if backend is None:
            raise ValueError("Backend reference required to fetch result")
        timeout = timeout or self.timeout
        start = time.time()
        while time.time() - start < timeout:
            state = backend.load_task_state(self.task_id)
            if state:
                status = state.get("status"); result = state.get("result")
                if status == "failed":
                    raise TaskProcessingError(self.task_name, result or "Task failed")
                if status == "completed":
                    # optional pydantic coercion
                    if returns is None and modelq_ref is not None:
                        func = getattr(modelq_ref, self.task_name, None)
                        returns = getattr(func, "_mq_returns", None)
                    if returns:
                        try:
                            if isinstance(result, str):
                                try:
                                    result_data = json.loads(result)
                                except Exception:
                                    result_data = result
                            else:
                                result_data = result
                            if isinstance(result_data, dict):
                                return returns(**result_data)
                            if isinstance(result_data, returns):
                                return result_data
                            return returns.parse_obj(result_data)
                        except Exception as ve:
                            raise TaskProcessingError(self.task_name, f"Result validation failed: {ve}")
                    return result
            time.sleep(1)
        raise TaskTimeoutError(self.task_id)

    # ─────────────────────── Helpers ─────────────────────────────────
    @staticmethod
    def _encode_image(img: Image.Image) -> str:
        buf = io.BytesIO(); img.save(buf, format="PNG")
        return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode()

    def _convert_to_string(self, data: Any) -> str:
        if isinstance(data, (dict, list, int, float, bool)):
            return json.dumps(data)
        if isinstance(data, (Image.Image, PngImagePlugin.PngImageFile)):
            return self._encode_image(data)
        return str(data)
