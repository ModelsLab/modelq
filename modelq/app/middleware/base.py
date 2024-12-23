# modelq/app/middleware.py

from abc import ABC, abstractmethod


class Middleware(ABC):
    def __init__(self) -> None:
        pass

    def execute(self, event, *args, **kwargs):
        if event == "before_worker_boot":
            self.before_worker_boot()
        elif event == "on_timeout":
            self.on_timeout(*args, **kwargs)
        # You can add more events here as needed

    @abstractmethod
    def before_worker_boot(self):
        """Called before the worker process starts up."""
        pass

    @abstractmethod
    def on_timeout(self, *args, **kwargs):
        """
        Called if the task times out. You can pass pipeline or anything else here.
        """
        pass
