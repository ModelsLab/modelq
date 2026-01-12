"""
Sentry Integration Demo for ModelQ

This example demonstrates how to integrate Sentry error tracking with ModelQ,
similar to how Sentry works with Celery.

Installation:
    pip install modelq[sentry]
    # or
    pip install modelq sentry-sdk

Setup:
    1. Create a Sentry account at https://sentry.io
    2. Create a new project (select Python)
    3. Copy the DSN from the project settings
    4. Set it as an environment variable or pass it directly

Environment Variables (recommended for production):
    export SENTRY_DSN="https://your-key@sentry.io/project-id"
    export SENTRY_ENVIRONMENT="production"

Usage:
    # Terminal 1 - Start worker with Sentry enabled
    python sentry_demo.py worker

    # Terminal 2 - Enqueue tasks
    python sentry_demo.py enqueue

    # Terminal 3 - Test error reporting
    python sentry_demo.py error
"""

import os
import sys
import time

from modelq import ModelQ

# Get Sentry DSN from environment or use None (Sentry will be disabled)
SENTRY_DSN = os.getenv("SENTRY_DSN")

# Initialize ModelQ with optional Sentry integration
mq = ModelQ(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    # Sentry configuration (all optional)
    sentry_dsn=SENTRY_DSN,
    sentry_traces_sample_rate=1.0,  # 100% of transactions for tracing (use lower in production)
    sentry_profiles_sample_rate=0.0,  # Disable profiling by default
    sentry_environment=os.getenv("SENTRY_ENVIRONMENT", "development"),
    sentry_release=os.getenv("SENTRY_RELEASE", "modelq-demo@1.0.0"),
    sentry_send_default_pii=False,  # Don't send PII by default
    sentry_debug=False,  # Set to True to see Sentry debug output
)


@mq.task()
def process_image(image_url: str, quality: int = 80):
    """Example task that processes an image."""
    print(f"Processing image: {image_url} with quality: {quality}")
    time.sleep(2)  # Simulate processing
    return {"status": "success", "url": image_url, "processed_quality": quality}


@mq.task()
def generate_text(prompt: str, max_tokens: int = 100):
    """Example task that generates text."""
    print(f"Generating text for prompt: {prompt[:50]}...")
    time.sleep(1)  # Simulate generation
    return {"text": f"Generated response for: {prompt}", "tokens_used": max_tokens}


@mq.task()
def failing_task(should_fail: bool = True):
    """
    Example task that always fails - useful for testing Sentry integration.
    
    When this task fails, you should see:
    - Error captured in Sentry with full stack trace
    - Task context (task_id, task_name, payload)
    - Worker information
    - Breadcrumbs showing task processing history
    """
    print("Starting failing task...")
    time.sleep(0.5)
    
    if should_fail:
        # This error will be captured and sent to Sentry
        raise ValueError("This is a test error for Sentry integration!")
    
    return {"status": "success"}


@mq.task()
def division_task(a: int, b: int):
    """Task that performs division - will fail if b is 0."""
    print(f"Dividing {a} by {b}")
    result = a / b  # Will raise ZeroDivisionError if b is 0
    return {"result": result}


@mq.task(retries=3)
def flaky_task(fail_probability: float = 0.7):
    """
    Task that randomly fails - demonstrates retry behavior with Sentry.
    
    Only the final failure (after all retries) will be reported to Sentry.
    """
    import random
    
    print(f"Running flaky task (fail probability: {fail_probability})")
    
    if random.random() < fail_probability:
        raise RuntimeError("Random failure in flaky task!")
    
    return {"status": "success", "message": "Task completed successfully!"}


def run_worker():
    """Start the ModelQ worker with Sentry integration."""
    print("=" * 60)
    print("Starting ModelQ Worker with Sentry Integration")
    print("=" * 60)
    
    if SENTRY_DSN:
        print(f"Sentry DSN: {SENTRY_DSN[:30]}...")
        print(f"Sentry Environment: {os.getenv('SENTRY_ENVIRONMENT', 'development')}")
    else:
        print("WARNING: SENTRY_DSN not set. Sentry integration is disabled.")
        print("Set SENTRY_DSN environment variable to enable error tracking.")
    
    print("-" * 60)
    print("Registered tasks:")
    for task_name in mq.allowed_tasks:
        print(f"  - {task_name}")
    print("-" * 60)
    
    mq.start_workers(no_of_workers=2)
    
    print("Workers started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down workers...")


def enqueue_tasks():
    """Enqueue some example tasks."""
    print("Enqueueing tasks...")
    
    # Enqueue a successful task
    task1 = process_image("https://example.com/image.jpg", quality=90)
    print(f"Enqueued process_image: {task1.task_id}")
    
    task2 = generate_text("Write a poem about machine learning")
    print(f"Enqueued generate_text: {task2.task_id}")
    
    print("\nTasks enqueued successfully!")
    print("Check the worker terminal to see processing output.")


def enqueue_error_tasks():
    """Enqueue tasks that will fail - useful for testing Sentry."""
    print("Enqueueing error-producing tasks for Sentry testing...")
    
    # Task that explicitly fails
    task1 = failing_task(should_fail=True)
    print(f"Enqueued failing_task: {task1.task_id}")
    
    # Task that will cause ZeroDivisionError
    task2 = division_task(10, 0)
    print(f"Enqueued division_task (will fail): {task2.task_id}")
    
    # Flaky task that may fail
    task3 = flaky_task(fail_probability=0.9)
    print(f"Enqueued flaky_task: {task3.task_id}")
    
    print("\nError tasks enqueued!")
    print("Check Sentry dashboard to see captured errors.")
    if SENTRY_DSN:
        print(f"Sentry Dashboard: https://sentry.io/")
    else:
        print("WARNING: SENTRY_DSN not set - errors won't be reported to Sentry")


def show_help():
    """Show usage help."""
    print(__doc__)
    print("\nCommands:")
    print("  python sentry_demo.py worker   - Start the worker")
    print("  python sentry_demo.py enqueue  - Enqueue successful tasks")
    print("  python sentry_demo.py error    - Enqueue failing tasks (test Sentry)")
    print("  python sentry_demo.py help     - Show this help message")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "worker":
        run_worker()
    elif command == "enqueue":
        enqueue_tasks()
    elif command == "error":
        enqueue_error_tasks()
    elif command == "help":
        show_help()
    else:
        print(f"Unknown command: {command}")
        show_help()
        sys.exit(1)
