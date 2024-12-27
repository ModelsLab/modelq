class TaskTimeoutError(Exception):
    """Custom exception to indicate task timeout."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task {task_id} timed out waiting for result.")
        self.task_id = task_id


class TaskProcessingError(Exception):
    """
    A custom exception indicating something went wrong processing the task.
    We only require one 'message' argument for simplicity.
    """
    def __init__(self, message: str):
        super().__init__(message)
