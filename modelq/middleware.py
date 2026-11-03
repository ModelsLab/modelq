"""Task processors and helpers available inside a running task."""

import asyncio
import copy
import inspect
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import ContextVar

from modelq.app.middleware import Middleware


logger = logging.getLogger(__name__)
_current_processor = ContextVar("modelq_processor", default=None)
_postprocessors = ThreadPoolExecutor(max_workers=4, thread_name_prefix="modelq-postprocessor")


class ProcessorMiddleware:
    """Subclass and implement start with the same inputs as its task or sender."""

    def start(self, *args, **kwargs):
        raise NotImplementedError


# Keep the spelling from the original processor API proposal available.
Processor_middlware = ProcessorMiddleware
Processor_middleware = ProcessorMiddleware


def _context():
    context = _current_processor.get()
    if context is None:
        raise RuntimeError("Processor helpers can only be used inside a running task")
    return context


def get_preprocessor_result():
    """Return this task attempt's preprocessor result (None if not configured)."""
    return _context().preprocessor_result


def send_postprocessor(*args, **kwargs):
    """Stage a call after successful task completion; return its concurrent Future.

    Arguments are copied now. Do not wait on the Future inside the task: the
    postprocessor is only submitted after the task result has been persisted.
    """
    return _context().stage(args, kwargs)


def _validate_processor(processor):
    if processor is not None and (
        not isinstance(processor, type) or not issubclass(processor, ProcessorMiddleware)
    ):
        raise TypeError("Processors must be ProcessorMiddleware subclasses")


def _start(processor, args, kwargs):
    result = processor().start(*args, **kwargs)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result


class _ProcessorExecution:
    """One attempt's state, shared with its timeout thread but no other task."""

    def __init__(self, preprocessor, postprocessor, prepared=False, result=None):
        self.preprocessor = preprocessor
        self.postprocessor = postprocessor
        self.preprocessor_result = result
        self._prepared = prepared
        self._pending = []
        self._lock = threading.Lock()
        self._closed = False

    def _prepare(self, args, kwargs):
        if self.preprocessor is not None and not self._prepared:
            # A processor can transform its own inputs without mutating the task.
            args, kwargs = copy.deepcopy((args, kwargs))
            self.preprocessor_result = _start(self.preprocessor, args, kwargs)
        with self._lock:
            if self._closed:
                raise RuntimeError("Task attempt has already ended")

    def run(self, function, *args, **kwargs):
        token = _current_processor.set(self)
        try:
            self._prepare(args, kwargs)
            return function(*args, **kwargs)
        finally:
            _current_processor.reset(token)

    def iterate(self, function, *args, **kwargs):
        token = _current_processor.set(self)
        try:
            self._prepare(args, kwargs)
            yield from function(*args, **kwargs)
        finally:
            _current_processor.reset(token)

    def stage(self, args, kwargs):
        if self.postprocessor is None:
            raise RuntimeError("No postprocessor is configured for this task")
        args, kwargs = copy.deepcopy((args, kwargs))
        future = Future()
        with self._lock:
            if self._closed:
                raise RuntimeError("Task attempt has already ended")
            self._pending.append((future, args, kwargs))
        return future

    def _process(self, future, args, kwargs):
        if not future.set_running_or_notify_cancel():
            return
        try:
            result = _start(self.postprocessor, args, kwargs)
        except BaseException as exc:
            future.set_exception(exc)
            logger.exception("ModelQ postprocessor failed")
        else:
            future.set_result(result)

    def finish(self, success=False):
        with self._lock:
            self._closed = True
            pending, self._pending = self._pending, []
        for future, args, kwargs in pending:
            if not success:
                future.cancel()
                continue
            try:
                _postprocessors.submit(self._process, future, args, kwargs)
            except Exception as exc:
                if future.set_running_or_notify_cancel():
                    future.set_exception(exc)
                logger.exception("Could not submit ModelQ postprocessor")


__all__ = [
    "Middleware", "ProcessorMiddleware", "Processor_middlware",
    "Processor_middleware", "get_preprocessor_result", "send_postprocessor",
]
