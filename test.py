# from celery_ml import CeleryML
# import time

# celery_ml = CeleryML()
# print(celery_ml)


# @celery_ml.task()
# def add(a, b):
#     # time.sleep(20)
#     return a + b

# celery_ml.start_worker()

# result_add = add(3, 4)
# print(f"Result of add(3, 4): {result_add}")


from celery_ml import CeleryML
import time
from celery_ml.exceptions import TaskTimeoutError

# Create an instance of CeleryML
celery_ml = CeleryML()

# Print the CeleryML instance to verify its creation
print(celery_ml)

# Define a task using CeleryML's task decorator with a timeout of 5 seconds
@celery_ml.task(timeout=15)
def add(a, b):
    # Simulate a long-running task by sleeping for 10 seconds
    time.sleep(10)
    return a + b

# Start the worker in a separate thread
celery_ml.start_worker()

try:
    # Enqueue the task with arguments, the task should time out
    result_add = add(3, 4)
    print(f"Result of add(3, 4): {result_add}")
except TaskTimeoutError as e:
    print(f"Task timed out: {e}")
except Exception as e:
    print(f"Task failed with an error: {e}")
