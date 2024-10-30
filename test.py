from celery_ml import CeleryML
import time

celery_ml = CeleryML()
print(celery_ml)


@celery_ml.task()
def add(a, b):
    # time.sleep(20)
    return a + b

celery_ml.start_worker()

result_add = add(3, 4)
print(f"Result of add(3, 4): {result_add}")