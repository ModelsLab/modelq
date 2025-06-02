class Middleware:
    def __init__(self) -> None:
        pass

    def execute(self, event, *args, **kwargs):
        if event == "before_worker_boot":
            self.before_worker_boot()
        elif event == "after_worker_boot":
            self.after_worker_boot()
        elif event == "before_worker_shutdown":
            self.before_worker_shutdown()
        elif event == "after_worker_shutdown":
            self.after_worker_shutdown()
        elif event == "before_enqueue":
            self.before_enqueue(*args, **kwargs)
        elif event == "after_enqueue":
            self.after_enqueue(*args, **kwargs)
        elif event == "on_timeout":
            self.on_timeout(*args, **kwargs)
        elif event == "on_error":
            self.on_error(*args, **kwargs)
        # Add more events as needed

    def before_worker_boot(self):
        """Called before the worker process starts up."""
        pass

    def after_worker_boot(self):
        """Called after the worker process has started."""
        pass

    def before_worker_shutdown(self):
        """Called right before the worker is about to shut down."""
        pass

    def after_worker_shutdown(self):
        """Called after the worker has shut down."""
        pass

    def before_enqueue(self, *args, **kwargs):
        """Called before a task is enqueued."""
        pass

    def after_enqueue(self, *args, **kwargs):
        """Called after a task is enqueued."""
        pass

    def on_timeout(self, *args, **kwargs):
        """Called when a task times out."""
        pass
        
    def on_error(self,*args,**kwwargs):
        """Called when a task throws an error."""
        pass