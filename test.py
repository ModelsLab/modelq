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

celery_ml = CeleryML()

print(celery_ml)

@celery_ml.task(timeout=15, stream=True)
def add_streaming(a, b, c):
    for i in range(1, 6):
        time.sleep(5)
        yield f"Intermediate result {i}: {a + b + c}"
    return a + b + c

@celery_ml.task(timeout=15)
def add(a, b, c):
    time.sleep(20)
    return a + b + c

celery_ml.start_worker()

try:
    # Testing regular task
    result_add = add(3, 4, 5)
    print(f"Result of add(3, 4, 5): {result_add}")

    # Testing streaming task
    result_add_streaming_task = add_streaming(1, 2, 3)
    output = result_add_streaming_task.get_stream(celery_ml.redis_client)
    print(output)
    for result in output:
        print("from here ")
        print(result)
except TaskTimeoutError as e:
    print(f"Task timed out: {e}")
except Exception as e:
    print(f"Task failed with an error: {e}")
