# ModelQ

ModelQ is a Python library designed specifically for scheduling and queuing machine learning inference tasks. It is built to be a more efficient alternative to existing task scheduling libraries, such as Celery, which may not always handle machine learning workloads effectively. ModelQ integrates Redis for backend data management, threading for concurrency, and uses task decorators to streamline the scheduling of tasks.

## Features
- **ML-Specific Task Scheduling**: Optimized to handle machine learning tasks, such as inferencing and model execution, with minimal overhead.
- **Redis-Based Queueing**: Uses Redis for fast, reliable queuing and task storage, ensuring scalability and efficiency.
- **Lightweight Threading**: Integrates Python threading for non-blocking operations and faster task execution.
- **Simple Task Decorators**: Easily turn Python functions into scheduled tasks with decorators, making your code concise and readable.
- **Flexible Usage**: Customize the queuing and scheduling mechanism to suit the requirements of different ML models or workflows.

## Installation

To install ModelQ, you can use pip:

```bash
pip install modelq
```

## Advanced Example

Here is a more advanced example demonstrating the use of ModelQ with retries, timeouts, and streaming tasks:

```python
from modelq import ModelQ
import time
from modelq.exceptions import TaskTimeoutError

# Initialize ModelQ
q_instance = ModelQ()

print(q_instance)

# Define a streaming task with retries and a timeout
@q_instance.task(timeout=15, stream=True, retries=2)
def add_streaming(a, b, c):
    for i in range(1, 6):
        time.sleep(5)
        yield f"Intermediate result {i}: {a + b + c}"
    return a + b + c

# Define a regular task with retries
@q_instance.task(timeout=15, retries=3)
def add(a, b, c):
    return [a + b + c]

# Start workers
q_instance.start_workers()

try:
    # Testing regular task with retry mechanism
    result_add = add(3, 4, 5)
    print(f"Result of add(3, 4, 5): {result_add}")
    output = result_add.get_result(q_instance.redis_client)
    print(output)

    # Testing streaming task with retry mechanism
    result_add_streaming_task = add_streaming(1, 2, 3)
    output = result_add_streaming_task.get_stream(q_instance.redis_client)
    for result in output:
        print(result)
except TaskTimeoutError as e:
    print(f"Task timed out: {e}")
```

## Configuration

ModelQ can be configured to connect to your Redis instance:

```python
modelq = ModelQ(redis_host='your_redis_host', redis_port=your_redis_port, redis_db=0)
```

## Roadmap
- **Support for GPU-based Tasks**: Integrate GPU awareness to enable targeted execution on GPU-based machines.
- **Priority Queueing**: Add priority levels to tasks to enable more urgent tasks to be executed sooner.
- **Fault Tolerance and Retries**: Automatic retries for failed tasks to enhance robustness.

## Contributing
We welcome contributions to ModelQ! If you have suggestions, feature requests, or bug reports, feel free to open an issue or submit a pull request on [GitHub](https://github.com/modelslab/modelq).

## License
ModelQ is licensed under the MIT License. See `LICENSE` for more information.

## Acknowledgements
- **Redis**: Used for backend queuing and task management.
- **Celery**: Inspiration for improving task management for machine learning-specific workloads.

## Contact
If you have any questions or need help, feel free to contact us at [your_email@example.com].
