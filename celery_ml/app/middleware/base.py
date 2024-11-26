from abc import ABC , abstractmethod


class Middleware(ABC) :
    def __init__(self) -> None:
        pass
    
    def execute(self,event):
        if event == "before_worker_boot":
            self.before_worker_boot()

    @abstractmethod
    def before_worker_boot(self):
        "Called before the worker process starts up."
        pass

    

