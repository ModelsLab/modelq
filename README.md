# ModelQ

![ModelQ Logo](assets/logo.PNG)

[![PyPI version](https://img.shields.io/pypi/v/modelq.svg)](https://pypi.org/project/modelq/)
[![Downloads](https://img.shields.io/pypi/dm/modelq.svg)](https://pypi.org/project/modelq/)

ModelQ is a lightweight Python library for scheduling and queuing machine learning inference tasks. It's designed as a faster and simpler alternative to Celery for ML workloads, using Redis and threading to efficiently run background tasks.

ModelQ is developed and maintained by the team at [Modelslab](https://modelslab.com/).

> **About Modelslab**: Modelslab provides powerful APIs for AI-native applications including:
> - Image generation
> - Uncensored chat
> - Video generation
> - Audio generation
> - And much more

---

## ✨ Features

- ✅ Retry support (automatic and manual)
- ⏱ Timeout handling for long-running tasks
- 🔁 Manual retry using `RetryTaskException`
- 🎮 Streaming results from tasks in real-time
- 🧹 Middleware hooks for task lifecycle events
- ⚡ Fast, non-blocking concurrency using threads
- 🧵 Built-in decorators to register tasks quickly
- 💃 Redis-based task queueing
- 🖥️ CLI interface for orchestration

---

## 🗆 Installation

```bash
pip install modelq
```

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

## ⚙️ Middleware Support

ModelQ allows you to plug in custom middleware to hook into events:

### Supported Events
- `before_worker_boot`
- `after_worker_boot`
- `before_worker_shutdown`
- `after_worker_shutdown`
- `before_enqueue`
- `after_enqueue`
- `on_error`

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

## 🛠️ Configuration

Connect to Redis using custom config:

```python
from redis import Redis

imagine_db = Redis(host="localhost", port=6379, db=0)
modelq = ModelQ(
    redis_client=imagine_db,
    delay_seconds=10,  # delay between retries
    webhook_url="https://your.error.receiver/discord-or-slack"
)
```

---

## 📜 License

ModelQ is released under the MIT License.

---

## 🤝 Contributing

We welcome contributions! Open an issue or submit a PR at [github.com/modelslab/modelq](https://github.com/modelslab/modelq).
