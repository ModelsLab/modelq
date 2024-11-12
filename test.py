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

@celery_ml.task(timeout=15)
def add(a, b,c):
    time.sleep(10)
    return a + b + c

celery_ml.start_worker()

try:
    result_add = add(3, 4,5)
    print(f"Result of add(3, 4,5): {result_add}")
except TaskTimeoutError as e:
    print(f"Task timed out: {e}")
except Exception as e:
    print(f"Task failed with an error: {e}")