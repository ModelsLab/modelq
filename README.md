<p align="center">
  <img src="https://github.com/user-attachments/assets/bd1908c3-d59d-4902-8c79-bf48869c1109" alt="ModelsLab Logo" />
</p>

<div align="center">
  <a href="https://discord.com/invite/modelslab-1033301189254729748">
    <img src="https://img.shields.io/badge/Discord-Join%20Us-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="Discord">
  </a>
  <a href="https://x.com/ModelsLabAI">
    <img src="https://img.shields.io/badge/X-@ModelsLabAI-000000?style=for-the-badge&logo=twitter&logoColor=white" alt="X/Twitter">
  </a>
  <a href="https://github.com/ModelsLab">
    <img src="https://img.shields.io/badge/GitHub-ModelsLab-181717?style=for-the-badge&logo=github&logoColor=white" alt="GitHub">
  </a>
</div>


# ModelQ

![ModelQ Logo](assets/logo.PNG)

[![PyPI version](https://img.shields.io/pypi/v/modelq.svg)](https://pypi.org/project/modelq/)
[![Downloads](https://img.shields.io/pypi/dm/modelq.svg)](https://pypi.org/project/modelq/)

ModelQ is a lightweight Python library for scheduling and queuing machine learning inference tasks. It's designed as a faster and simpler alternative to Celery for ML workloads, using Redis and threading to efficiently run background tasks.

ModelQ is developed and maintained by the team at [Modelslab](https://modelslab.com/).

> **About Modelslab**: Modelslab provides powerful APIs for AI-native applications including:
>
> * Image generation
> * Uncensored chat
> * Video generation
> * Audio generation
> * And much more

---

## ✨ Features

* ✅ Retry support (automatic and manual)
* ⏱ Timeout handling for long-running tasks
* 🔁 Manual retry using `RetryTaskException`
* 🎮 Streaming results from tasks in real-time
* 🧹 Middleware hooks for task lifecycle events
* ⚡ Fast, non-blocking concurrency using threads
* 🧵 Built-in decorators to register tasks quickly
* 💃 Redis-based task queueing
* 🖥️ CLI interface for orchestration
* 🔢 Pydantic model support for task validation and typing
* 🌐 Auto-generated REST API for tasks
* 🚫 Task cancellation for queued or running tasks
* 📊 Progress tracking for long-running tasks
* 📜 Task history with configurable retention

---

## 🗆 Installation

```bash
pip install modelq
```

---

## 🚀 Auto-Generated REST API

One of ModelQ's most powerful features is the ability to **expose your tasks as HTTP endpoints automatically**.

By running a single command, every registered task becomes an API route:

```bash
modelq serve-api --app-path main:modelq_app --host 0.0.0.0 --port 8000
```

### How It Works

* Each task registered with `@q.task(...)` is turned into a POST endpoint under `/tasks/{task_name}`
* If your task uses Pydantic input/output, the endpoint will validate the request and return a proper response schema
* The API is built using FastAPI, so you get automatic Swagger docs at:

```
http://localhost:8000/docs
```

### Example Usage

```bash
curl -X POST http://localhost:8000/tasks/add \
  -H "Content-Type: application/json" \
  -d '{"a": 3, "b": 7}'
```

You can now build ML inference APIs without needing to write any web code!

---

## 🖥️ CLI Usage

You can interact with ModelQ using the `modelq` command-line tool. All commands require an `--app-path` parameter to locate your ModelQ instance in `module:object` format.

### Start Workers

```bash
modelq run-workers main:modelq_app --workers 2
```

Start background worker threads for executing tasks.

### Check Queue Status

```bash
modelq status --app-path main:modelq_app
```

Show number of servers, queued tasks, and registered task types.

### List Queued Tasks

```bash
modelq list-queued --app-path main:modelq_app
```

Display a list of all currently queued task IDs and their names.

### Clear the Queue

```bash
modelq clear-queue --app-path main:modelq_app
```

Remove all tasks from the queue.

### Remove a Specific Task

```bash
modelq remove-task --app-path main:modelq_app --task-id <task_id>
```

Remove a specific task from the queue by ID.

### Serve API

```bash
modelq serve-api --app-path main:modelq_app --host 0.0.0.0 --port 8000 --log-level info
```

Start a FastAPI server for ModelQ to accept task submissions over HTTP.

### Version

```bash
modelq version
```

Print the current version of ModelQ CLI.

More commands like `requeue-stuck`, `prune-results`, and `get-task-status` are coming soon.

---

## 🧠 Basic Usage

```python
from modelq import ModelQ
from modelq.exceptions import RetryTaskException
from redis import Redis
import time

imagine_db = Redis(host="localhost", port=6379, db=0)
q = ModelQ(redis_client=imagine_db)

@q.task(timeout=10, retries=2)
def add(a, b):
    return a + b

@q.task(stream=True)
def stream_multiples(x):
    for i in range(5):
        time.sleep(1)
        yield f"{i+1} * {x} = {(i+1) * x}"

@q.task()
def fragile(x):
    if x < 5:
        raise RetryTaskException("Try again.")
    return x

q.start_workers()

task = add(2, 3)
print(task.get_result(q.redis_client))
```

---

## 🔑 Custom Task IDs

By default, ModelQ generates a UUID for each task. You can provide your own task ID using the `_task_id` parameter to correlate tasks with your database records:

```python
from modelq import ModelQ
from redis import Redis

redis_client = Redis(host="localhost", port=6379, db=0)
mq = ModelQ(redis_client=redis_client)

@mq.task()
def process_order(order_data: dict):
    # Process the order...
    return {"status": "completed"}

mq.start_workers()

# Use your database record ID as the task ID
order_id = "order-12345"
task = process_order({"item": "widget"}, _task_id=order_id)

print(task.task_id)  # 'order-12345'

# Later, retrieve the task using the same ID
status = mq.get_task_status(order_id)
details = mq.get_task_details(order_id)
```

This is useful when you want to:
- Track tasks using your existing database primary keys
- Easily correlate queue tasks with database records
- Look up task status without storing the generated UUID

---

## 🔢 Pydantic Support

ModelQ supports **Pydantic models** as both input and output types for tasks. This allows automatic validation of input parameters and structured return values.

### Example

```python
from pydantic import BaseModel, Field
from redis import Redis
from modelq import ModelQ
import time

class AddIn(BaseModel):
    a: int = Field(ge=0)
    b: int = Field(ge=0)

class AddOut(BaseModel):
    total: int

redis_client = Redis(host="localhost", port=6379, db=0)
mq = ModelQ(redis_client=redis_client)

@mq.task(schema=AddIn, returns=AddOut)
def add(payload: AddIn) -> AddOut:
    print(f"Processing addition: {payload.a} + {payload.b}.")
    time.sleep(10)  # Simulate some processing time
    return AddOut(total=payload.a + payload.b)
```

### Getting Result

```python
output = job.get_result(mq.redis_client, returns=AddOut)
```

ModelQ will validate inputs using Pydantic and serialize/deserialize results seamlessly.

---

## ⚙️ Middleware Support

ModelQ allows you to plug in custom middleware to hook into events:

### Supported Events

* `before_worker_boot`
* `after_worker_boot`
* `before_worker_shutdown`
* `after_worker_shutdown`
* `before_enqueue`
* `after_enqueue`
* `on_error`

### Example

```python
from modelq.app.middleware import Middleware

class LoggingMiddleware(Middleware):
    def before_enqueue(self, *args, **kwargs):
        print("Task about to be enqueued")

    def on_error(self, task, error):
        print(f"Error in task {task.task_id}: {error}")
```

Attach to ModelQ instance:

```python
q.middleware = LoggingMiddleware()
```

---

## 🚫 Task Cancellation

ModelQ supports cancelling tasks that are queued or in progress. This is useful for long-running ML inference tasks that need to be stopped.

### Cancelling a Task

```python
from modelq import ModelQ
from redis import Redis

redis_client = Redis(host="localhost", port=6379, db=0)
mq = ModelQ(redis_client=redis_client)

# Enqueue a task
task = my_long_task({"data": "value"})

# Cancel the task
cancelled = mq.cancel_task(task.task_id)
if cancelled:
    print(f"Task {task.task_id} was cancelled")
```

### Checking Cancellation Status

```python
# Check if a task was cancelled
if mq.is_task_cancelled(task.task_id):
    print("Task was cancelled")

# Get all cancelled tasks
cancelled_tasks = mq.get_cancelled_tasks(limit=100)
for t in cancelled_tasks:
    print(f"Cancelled: {t['task_id']} - {t['task_name']}")
```

### Handling Cancellation Inside a Task

For long-running tasks, you should periodically check for cancellation and exit gracefully:

```python
@mq.task()
def long_running_task(params: dict):
    items = params.get("items", [])
    results = []

    for i, item in enumerate(items):
        # Check if task was cancelled
        task_id = params.get("_task_id")  # Task ID is injected
        if task_id and mq.is_task_cancelled(task_id):
            return {"status": "cancelled", "processed": i}

        # Process item
        result = process_item(item)
        results.append(result)

    return {"status": "completed", "results": results}
```

### Cancellation in Streaming Tasks

Streaming tasks automatically check for cancellation and will stop yielding results:

```python
task = my_streaming_task({"prompt": "Generate text"})

# Start consuming stream in another thread/process
# ...

# Cancel from main thread
mq.cancel_task(task.task_id)
# The stream will stop gracefully
```

---

## 📊 Progress Tracking

For long-running tasks, you can report progress to let clients know how far along the task is.

### Reporting Progress Inside a Task

```python
@mq.task()
def train_model(params: dict):
    task_id = params.get("_task_id")
    epochs = params.get("epochs", 10)

    for epoch in range(epochs):
        # Report progress (0.0 to 1.0)
        progress = (epoch + 1) / epochs
        mq.report_progress(task_id, progress, f"Training epoch {epoch + 1}/{epochs}")

        # Do actual training
        train_epoch(epoch)

    return {"status": "completed", "epochs": epochs}
```

### Getting Progress from Client Side

```python
import time

task = train_model({"epochs": 100})

# Poll for progress
while True:
    progress = mq.get_task_progress(task.task_id)
    if progress:
        print(f"Progress: {progress['progress'] * 100:.1f}% - {progress['message']}")

    # Check if task is done
    details = mq.get_task_details(task.task_id)
    if details and details['status'] in ['completed', 'failed']:
        break

    time.sleep(1)

# Get final result
result = task.get_result(mq.redis_client)
```

### Progress via Task Object

You can also get progress directly from the task object:

```python
task = train_model({"epochs": 100})

# Get progress using task method
progress = task.get_progress(mq.redis_client)
if progress:
    print(f"Progress: {progress['progress'] * 100:.1f}%")
    print(f"Message: {progress['message']}")
    print(f"Updated at: {progress['updated_at']}")
```

### Combining Progress with Cancellation

```python
@mq.task()
def process_large_dataset(params: dict):
    task_id = params.get("_task_id")
    items = params.get("items", [])
    total = len(items)
    results = []

    for i, item in enumerate(items):
        # Check cancellation
        if mq.is_task_cancelled(task_id):
            mq.report_progress(task_id, i / total, "Cancelled by user")
            return {"status": "cancelled", "processed": i}

        # Report progress
        mq.report_progress(task_id, i / total, f"Processing item {i + 1}/{total}")

        # Process
        results.append(process(item))

    mq.report_progress(task_id, 1.0, "Completed")
    return {"status": "completed", "results": results}
```

---

## 🖥️ Worker Info

Get detailed information about registered workers including system resources (CPU, RAM, GPU). This is useful for monitoring your worker fleet and understanding resource utilization.

### Getting All Workers

```python
from modelq import ModelQ
from redis import Redis

redis_client = Redis(host="localhost", port=6379, db=0)
mq = ModelQ(redis_client=redis_client)

# Get all registered workers
workers = mq.get_workers()
for worker_id, worker in workers.items():
    print(f"Worker: {worker_id}")
    print(f"  Status: {worker['status']}")
    print(f"  Hostname: {worker['hostname']}")
    print(f"  OS: {worker['os']}")
    print(f"  Python: {worker['python_version']}")
    
    if worker['system_info']:
        cpu = worker['system_info']['cpu']
        ram = worker['system_info']['ram']
        
        print(f"  CPU: {cpu['cores_logical']} cores ({cpu['usage_percent']}% used)")
        print(f"  RAM: {ram['total_gb']} GB ({ram['used_percent']}% used)")
        
        # GPU info (if available)
        for gpu in worker['system_info']['gpu']:
            print(f"  GPU: {gpu['name']} - {gpu['memory_total_gb']} GB")
            print(f"       Utilization: {gpu['gpu_utilization_percent']}%")
            print(f"       Memory: {gpu['memory_used_gb']}/{gpu['memory_total_gb']} GB")
    
    print(f"  Tasks: {', '.join(worker['allowed_tasks'])}")
```

### Getting a Specific Worker

```python
# Get a specific worker by ID
worker = mq.get_worker('gpu-server-1')
if worker:
    print(f"Worker {worker['worker_id']} is {worker['status']}")
    if worker['system_info']['gpu']:
        gpu = worker['system_info']['gpu'][0]
        print(f"GPU Memory Free: {gpu['memory_free_gb']} GB")
```

### Worker Info Fields

Each worker includes the following information:

| Field | Description |
|-------|-------------|
| `worker_id` | Unique worker identifier |
| `status` | Current status (idle, busy) |
| `allowed_tasks` | List of tasks this worker handles |
| `last_heartbeat` | Unix timestamp of last heartbeat |
| `hostname` | Worker hostname |
| `os` | Operating system info |
| `python_version` | Python version |
| `system_info` | Detailed CPU, RAM, and GPU information |

### System Info Structure

The `system_info` field contains:

```python
{
    "cpu": {
        "cores_physical": 8,
        "cores_logical": 16,
        "usage_percent": 45.2,
        "freq_mhz": 3200.0
    },
    "ram": {
        "total_gb": 64.0,
        "available_gb": 32.5,
        "used_percent": 49.2
    },
    "gpu": [
        {
            "index": 0,
            "name": "NVIDIA RTX 4090",
            "memory_total_gb": 24.0,
            "memory_used_gb": 8.5,
            "memory_free_gb": 15.5,
            "gpu_utilization_percent": 75,
            "memory_utilization_percent": 35
        }
    ]
}
```

---

## 🛠️ Configuration

Connect to Redis using custom config:

```python
from redis import Redis

imagine_db = Redis(host="localhost", port=6379, db=0)
modelq = ModelQ(
    redis_client=imagine_db,
    delay_seconds=10,  # delay between retries
    webhook_url="https://your.error.receiver/discord-or-slack",
    task_history_retention=86400,  # task history retention in seconds (default: 24 hours)
    task_ttl=86400,  # task TTL in seconds (default: 24 hours)
)
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `redis_client` | Required | Redis client instance |
| `delay_seconds` | `10` | Delay between task retries |
| `webhook_url` | `None` | URL for error notifications (Discord/Slack) |
| `task_history_retention` | `86400` (24h) | How long to keep task history in seconds |
| `task_ttl` | `86400` (24h) | Task time-to-live in seconds |

### Cleanup Expired Tasks

Tasks older than the TTL can be cleaned up manually:

```python
# Remove expired tasks from the queue
removed_count = mq.cleanup_expired_tasks()
print(f"Removed {removed_count} expired tasks")

# Clear old task history
removed_count = mq.clear_task_history()  # Uses configured retention
print(f"Cleared {removed_count} old history entries")

# Or specify custom age in seconds
removed_count = mq.clear_task_history(3600)  # Clear tasks older than 1 hour
```

---

## 📜 License

ModelQ is released under the MIT License.

---

## 🤝 Contributing

We welcome contributions! Open an issue or submit a PR at [github.com/modelslab/modelq](https://github.com/modelslab/modelq).
