class TaskTimeoutError(Exception):
    """Custom exception to indicate task timeout."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task {task_id} timed out waiting for result.")
        self.task_id = task_id


class TaskProcessingError(Exception):
    """Custom exception to indicate an error occurred during task processing."""

    def __init__(self, task_name: str, message: str):
        super().__init__(f"Error processing task {task_name}: {message}")
        self.task_name = task_name
        self.message = message


class RetryTaskException(Exception):
    """
    Exception to be raised within a task function to manually trigger a retry.
    """
    pass