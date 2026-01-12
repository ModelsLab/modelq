"""
Optional Sentry integration for ModelQ.

This module provides Sentry error tracking integration for ModelQ,
similar to how Sentry integrates with Celery.

Usage:
    from modelq import ModelQ

    mq = ModelQ(
        host="localhost",
        sentry_dsn="https://your-sentry-dsn@sentry.io/project-id",
        sentry_traces_sample_rate=1.0,  # optional
        sentry_environment="production",  # optional
    )
"""

import logging
import sys
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Flag to track if sentry-sdk is available
_sentry_available = False

try:
    import sentry_sdk
    from sentry_sdk import capture_exception, set_tag, set_context, set_user
    _sentry_available = True
except ImportError:
    sentry_sdk = None
    capture_exception = None
    set_tag = None
    set_context = None
    set_user = None


def is_sentry_available() -> bool:
    """Check if sentry-sdk is installed and available."""
    return _sentry_available


def init_sentry(
    dsn: str,
    traces_sample_rate: float = 0.0,
    profiles_sample_rate: float = 0.0,
    environment: Optional[str] = None,
    release: Optional[str] = None,
    server_name: Optional[str] = None,
    send_default_pii: bool = False,
    debug: bool = False,
    **kwargs,
) -> bool:
    """
    Initialize Sentry SDK for ModelQ.

    Args:
        dsn: Sentry DSN (Data Source Name)
        traces_sample_rate: Sample rate for performance tracing (0.0 to 1.0)
        profiles_sample_rate: Sample rate for profiling (0.0 to 1.0)
        environment: Environment name (e.g., 'production', 'staging')
        release: Release version string
        server_name: Server/worker name
        send_default_pii: Whether to send PII data
        debug: Enable Sentry debug mode
        **kwargs: Additional arguments passed to sentry_sdk.init()

    Returns:
        bool: True if Sentry was initialized successfully, False otherwise
    """
    if not _sentry_available:
        logger.warning(
            "sentry-sdk is not installed. Install it with: pip install sentry-sdk"
        )
        return False

    if not dsn:
        logger.warning("Sentry DSN not provided. Sentry integration disabled.")
        return False

    try:
        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=traces_sample_rate,
            profiles_sample_rate=profiles_sample_rate,
            environment=environment,
            release=release,
            server_name=server_name,
            send_default_pii=send_default_pii,
            debug=debug,
            **kwargs,
        )
        logger.info(f"Sentry initialized successfully (environment: {environment})")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {e}")
        return False


def capture_task_exception(
    exc: Exception,
    task_id: str,
    task_name: str,
    payload: Optional[Dict[str, Any]] = None,
    worker_id: Optional[str] = None,
    additional_context: Optional[Dict[str, Any]] = None,
    exc_info: tuple = None,
) -> Optional[str]:
    """
    Capture a task exception and send it to Sentry with full context.
    
    Sentry automatically captures:
    - Full stack trace with file names and line numbers
    - Local variables at each frame
    - Exception type and message

    Args:
        exc: The exception that occurred
        task_id: The task ID
        task_name: The task name
        payload: The task payload (will be sanitized)
        worker_id: The worker ID processing the task
        additional_context: Any additional context to include
        exc_info: Optional tuple from sys.exc_info() for complete traceback

    Returns:
        str: Sentry event ID if captured, None otherwise
    """
    if not _sentry_available or sentry_sdk is None:
        return None
    
    # Check if Sentry client is initialized
    try:
        if sentry_sdk.Hub.current.client is None:
            return None
    except Exception:
        return None

    try:
        with sentry_sdk.push_scope() as scope:
            # Set task-specific tags for filtering in Sentry
            scope.set_tag("task_name", task_name)
            scope.set_tag("task_id", task_id)
            if worker_id:
                scope.set_tag("worker_id", worker_id)

            # Set task context with detailed info
            scope.set_context("task", {
                "task_id": task_id,
                "task_name": task_name,
                "worker_id": worker_id,
            })

            # Add payload context (be careful with sensitive data)
            if payload:
                # Sanitize payload - remove potentially sensitive keys
                sanitized_payload = _sanitize_payload(payload)
                scope.set_context("task_payload", sanitized_payload)

            # Add any additional context
            if additional_context:
                scope.set_context("additional", additional_context)

            # Capture the exception with full traceback
            # Using exc_info ensures we get complete stack trace with line numbers
            if exc_info:
                event_id = sentry_sdk.capture_exception(exc_info)
            else:
                # Fallback: try to get current exc_info if we're in an except block
                current_exc_info = sys.exc_info()
                if current_exc_info[0] is not None:
                    event_id = sentry_sdk.capture_exception(current_exc_info)
                else:
                    event_id = sentry_sdk.capture_exception(exc)
            
            return event_id

    except Exception as e:
        logger.error(f"Failed to capture exception in Sentry: {e}")
        return None


def capture_task_message(
    message: str,
    level: str = "info",
    task_id: Optional[str] = None,
    task_name: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Capture a message and send it to Sentry.

    Args:
        message: The message to capture
        level: Log level ('debug', 'info', 'warning', 'error', 'fatal')
        task_id: Optional task ID
        task_name: Optional task name
        extra: Additional data to include

    Returns:
        str: Sentry event ID if captured, None otherwise
    """
    if not _sentry_available or sentry_sdk is None:
        return None
    
    try:
        if sentry_sdk.Hub.current.client is None:
            return None
    except Exception:
        return None

    try:
        with sentry_sdk.push_scope() as scope:
            if task_id:
                scope.set_tag("task_id", task_id)
            if task_name:
                scope.set_tag("task_name", task_name)
            if extra:
                for key, value in extra.items():
                    scope.set_extra(key, value)

            event_id = sentry_sdk.capture_message(message, level=level)
            return event_id

    except Exception as e:
        logger.error(f"Failed to capture message in Sentry: {e}")
        return None


def set_task_user(
    user_id: Optional[str] = None,
    email: Optional[str] = None,
    username: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> None:
    """
    Set user context for Sentry events.

    Args:
        user_id: User ID
        email: User email
        username: Username
        ip_address: User IP address
    """
    if not _sentry_available or sentry_sdk is None:
        return
    
    try:
        if sentry_sdk.Hub.current.client is None:
            return
    except Exception:
        return

    user_data = {}
    if user_id:
        user_data["id"] = user_id
    if email:
        user_data["email"] = email
    if username:
        user_data["username"] = username
    if ip_address:
        user_data["ip_address"] = ip_address

    if user_data:
        sentry_sdk.set_user(user_data)


def add_breadcrumb(
    message: str,
    category: str = "modelq",
    level: str = "info",
    data: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Add a breadcrumb to the current Sentry scope.

    Breadcrumbs are used to record events leading up to an error.

    Args:
        message: Breadcrumb message
        category: Category for the breadcrumb
        level: Log level
        data: Additional data
    """
    if not _sentry_available or sentry_sdk is None:
        return
    
    try:
        if sentry_sdk.Hub.current.client is None:
            return
    except Exception:
        return

    sentry_sdk.add_breadcrumb(
        message=message,
        category=category,
        level=level,
        data=data,
    )


def start_transaction(
    name: str,
    op: str = "task",
    description: Optional[str] = None,
):
    """
    Start a Sentry transaction for performance monitoring.

    Args:
        name: Transaction name
        op: Operation type
        description: Transaction description

    Returns:
        Transaction object or None if Sentry is not available
    """
    if not _sentry_available or sentry_sdk is None:
        return None
    
    try:
        if sentry_sdk.Hub.current.client is None:
            return None
    except Exception:
        return None

    return sentry_sdk.start_transaction(
        name=name,
        op=op,
        description=description,
    )


def _sanitize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize payload to remove potentially sensitive data.

    Args:
        payload: The payload to sanitize

    Returns:
        Sanitized payload
    """
    sensitive_keys = {
        "password", "passwd", "secret", "token", "api_key", "apikey",
        "auth", "authorization", "credential", "private_key", "privatekey",
        "access_token", "refresh_token", "session", "cookie",
    }

    def _sanitize(data: Any, depth: int = 0) -> Any:
        if depth > 10:  # Prevent infinite recursion
            return "[TRUNCATED]"

        if isinstance(data, dict):
            sanitized = {}
            for key, value in data.items():
                key_lower = key.lower()
                if any(s in key_lower for s in sensitive_keys):
                    sanitized[key] = "[REDACTED]"
                else:
                    sanitized[key] = _sanitize(value, depth + 1)
            return sanitized
        elif isinstance(data, list):
            return [_sanitize(item, depth + 1) for item in data[:100]]  # Limit list size
        elif isinstance(data, str) and len(data) > 1000:
            return data[:1000] + "...[TRUNCATED]"
        else:
            return data

    return _sanitize(payload)


def flush_sentry(timeout: float = 2.0) -> None:
    """
    Flush pending Sentry events.

    Call this before worker shutdown to ensure all events are sent.

    Args:
        timeout: Maximum time to wait for flush
    """
    if not _sentry_available or sentry_sdk is None:
        return
    
    try:
        if sentry_sdk.Hub.current.client is None:
            return
    except Exception:
        return

    try:
        sentry_sdk.flush(timeout=timeout)
    except Exception as e:
        logger.error(f"Failed to flush Sentry events: {e}")
