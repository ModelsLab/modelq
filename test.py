from pydantic import BaseModel, Field
from modelq import ModelQ
from redis import Redis

class AddIn(BaseModel):
    a: int = Field(ge=0)
    b: int = Field(ge=0)

class AddOut(BaseModel):
    total: int

redis_client = Redis(host="localhost", port=6379, db=0)
mq = ModelQ(redis_client = redis_client)


@mq.task(schema=AddIn, returns=AddOut, timeout=5)
def add(payload: AddIn) -> AddOut:
    return AddOut(total=payload.a + payload.b)

job = add(a=3, b=4)          # ✨ validated on the spot


import time

if __name__ == "__main__":
    mq.start_workers()

    # Keep the worker running indefinitely
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")