from modelq import ModelQ
import time
from modelq.exceptions import TaskTimeoutError

celery_ml = ModelQ()

print(celery_ml)


@celery_ml.task(timeout=15, stream=True, retries=2)
def add_streaming(a, b, c):
    for i in range(1, 6):
        time.sleep(5)
        yield f"Intermediate result {i}: {a + b + c}"
    return a + b + c


@celery_ml.task(timeout=15, retries=3)
def add(a, b, c):
    # This will trigger a timeout error to test retries
    # raise Exception("Lmao")
    return [a + b + c]


celery_ml.start_workers()

try:
    # Testing regular task with retry mechanism
    result_add = add(3, 4, 5)
    print(f"Result of add(3, 4, 5): {result_add}")
    output = result_add.get_result(celery_ml.redis_client)
    print(output)

    # Testing streaming task with retry mechanism
    # result_add_streaming_task = add_streaming(1, 2, 3)
    # output = result_add_streaming_task.get_stream(celery_ml.redis_client)
    # print(output)
    # for result in output:
    #     print("from here ")
    #     print(result)
except TaskTimeoutError as e:
    print(f"Task timed out: {e}")
