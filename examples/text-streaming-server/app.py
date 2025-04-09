from tasks import modelq_app, stream
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, Response

app = FastAPI()

@app.get("/completion/{question}")
async def completion(question: str):

    task = stream(question)

    return StreamingResponse(
        task.get_stream(modelq_app.redis_client), media_type="text/event-stream"
    )