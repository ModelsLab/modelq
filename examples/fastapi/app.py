from tasks import modelq , stream , add_task
from fastapi import FastAPI 
from fastapi.responses import StreamingResponse

app = FastAPI()

@app.get("/completion/{question}")
async def completion(question: str):

    task = stream(question)
        
    return StreamingResponse(task.get_stream(modelq.redis_client), media_type="text/event-stream")

@app.get("/add")
async def add():
    task = add_task()
    return str(task.get_result(modelq.redis_client))