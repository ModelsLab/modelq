from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class QueueBackend(ABC):
    """Abstract interface for ModelQ queue backends."""

    # ─── Task Enqueue/Dequeue ──────────────────────────────────────────────

    @abstractmethod
    def enqueue_task(self, task_data: dict) -> None:
        """Push a new task to the task queue."""
        pass

    @abstractmethod
    def dequeue_task(self, timeout: Optional[int] = None) -> Optional[dict]:
        """Pop the next task from the queue (blocking or timed)."""
        pass

    @abstractmethod
    def requeue_task(self, task_data: dict) -> None:
        """Re-queue an existing task (e.g., after failure or rejection)."""
        pass

    @abstractmethod
    def enqueue_delayed_task(self, task_data: dict, delay_seconds: int) -> None:
        """Push task to delayed queue (sorted by run timestamp)."""
        pass

    @abstractmethod
    def dequeue_ready_delayed_tasks(self) -> list:
        """Get all delayed tasks ready to run now (score <= time.time())."""
        pass

    @abstractmethod
    def flush_queue(self) -> None:
        """Empty all tasks from the main task queue (for tests/dev reset)."""
        pass

    # ─── Task Status Management ────────────────────────────────────────────

    @abstractmethod
    def save_task_state(self, task_id: str, task_data: dict, result: bool) -> None:
        """Save or update the final state of a task (completed/failed/etc)."""
        pass

    @abstractmethod
    def load_task_state(self, task_id: str) -> Optional[dict]:
        """Fetch a task's full state from storage."""
        pass

    @abstractmethod
    def remove_task_from_queue(self, task_id: str) -> bool:
        """Remove task from queue if still queued."""
        pass

    @abstractmethod
    def mark_processing(self, task_id: str) -> bool:
        """Add task to 'processing' set; return False if already processing."""
        pass

    @abstractmethod
    def unmark_processing(self, task_id: str) -> None:
        """Remove task from processing set."""
        pass

    @abstractmethod
    def get_all_processing_tasks(self) -> list:
        """Return list of currently 'processing' task IDs."""
        pass

    @abstractmethod
    def get_all_queued_tasks(self) -> list:
        """Return list of all tasks in the main queue."""
        pass

    # ─── Server Registry ───────────────────────────────────────────────────

    @abstractmethod
    def register_server(self, server_id: str, task_names: list) -> None:
        """Register a worker with allowed task names and heartbeat."""
        pass

    @abstractmethod
    def update_server_status(self, server_id: str, status: str) -> None:
        """Update current server status and heartbeat time."""
        pass

    @abstractmethod
    def get_all_server_ids(self) -> list:
        """Return all currently registered server IDs."""
        pass

    @abstractmethod
    def get_server_data(self, server_id: str) -> Optional[dict]:
        """Get full data object for a server."""
        pass

    @abstractmethod
    def prune_dead_servers(self, timeout: int) -> list:
        """Remove any servers whose heartbeat is older than `timeout` seconds."""
        pass

    # ─── Metrics + Maintenance ─────────────────────────────────────────────

    @abstractmethod
    def prune_old_results(self, older_than_seconds: int) -> int:
        """Delete old task results beyond TTL."""
        pass

    @abstractmethod
    def queue_length(self) -> int:
        """Return the length of the main task queue."""
        pass

    @abstractmethod
    def cleanup_dlq(self) -> None:
        """Clear all items from dead letter queue."""
        pass
