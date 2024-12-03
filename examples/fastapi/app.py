from tasks import modelq, stream, add_task, image_task
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, Response
from PIL import Image
import io
import time
from modelq.app.utils import base64_to_image

app = FastAPI()

modelq.get_all_queued_tasks()


def image_to_bytes(image: Image.Image) -> bytes:
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format="PNG")
    return img_byte_arr.getvalue()


@app.get("/completion/{question}")
async def completion(question: str):

    task = stream(question)

    return StreamingResponse(
        task.get_stream(modelq.redis_client), media_type="text/event-stream"
    )


@app.get("/get_image")
async def image_app():
    task = image_task()

    result = task.get_result(modelq.redis_client)
    # print(result)
    result = base64_to_image(result)
    if isinstance(result, Image.Image):
        img_byte_arr = image_to_bytes(result)
        return Response(content=img_byte_arr, media_type="image/png")
    return Response(content="Task result is not an image", media_type="text/plain")
    ## i want to return image here e


@app.get("/add")
async def add():
    task = add_task()
    modelq.get_all_queued_tasks()
    return str(task.get_result(modelq.redis_client))
