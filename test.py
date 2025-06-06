from pydantic import BaseModel, Field
from modelq import ModelQ
from redis import Redis
from modelq.app.backends.redis import RedisQueueBackend

class AddIn(BaseModel):
    a: int = Field(ge=0)
    b: int = Field(ge=0)

class AddOut(BaseModel):
    total: int

redis_client = Redis(host="localhost", port=6379, db=0)

backend = RedisQueueBackend(redis_client)
mq = ModelQ(backend=backend)


@mq.task(schema=AddIn, returns=AddOut)
def add(payload: AddIn) -> AddOut:
    print(f"Processing addition: {payload.a} + {payload.b}")
    time.sleep(10)  # Simulate some processing time
    return AddOut(total=payload.a + payload.b)

@mq.task()
def sub(a: int, b: int):
    print(f"Processing subtraction: {a} - {b}")
    return a - b

@mq.task()
def image_task(params: dict):
    print(f"Processing image task with params: {params}")
    # Simulate image processing
    return "Image processed successfully"

job = add(a=3, b=4)          # ✨ validated on the spot

job2 = sub(a=10, b=5)             # ✨ no schema validation, just a simple task

task = image_task({"image": "example.png"})  # ✨ no schema validation, just a simple task
task2 = image_task(params={"image": "example.png"}) 
import time

if __name__ == "__main__":
    mq.start_workers()

    # Keep the worker running indefinitely
    try:
        while True:
            output  = job.get_result(returns=AddOut)

            print(f"Result of addition: {output}")
            print(type(output))
            print(f"Result of addition (total): {output.total}")

            output2 = job2.get_result()
            print(f"Result of subtraction: {output2}")

            output3 = task.get_result(mq.redis_client)
            print(f"Result of image task: {output3}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")