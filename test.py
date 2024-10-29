from celery_ml import CeleryML

celery_ml = CeleryML()
print(celery_ml)


@celery_ml.task()
def add(a, b):
    return a + b


result_add = add(3, 4)
print(f"Result of add(3, 4): {result_add}")