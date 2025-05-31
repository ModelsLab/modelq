from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse,StreamingResponse
from pydantic import BaseModel
import uvicorn, json, gc, time
from typing import Dict, Callable, Any
from modelq.exceptions import TaskTimeoutError

# ------------------------------------------------------------------
#  Discover every wrapper produced by @mq.task in the current process
# ------------------------------------------------------------------
def _discover_task_wrappers() -> Dict[str, Callable[..., Any]]:
    wrappers: Dict[str, Callable[..., Any]] = {}
    for obj in gc.get_objects():
        if callable(obj) and hasattr(obj, "__wrapped__"):
            inner = getattr(obj, "__wrapped__", None)
            if inner and hasattr(inner, "_mq_schema"):
                wrappers[inner.__name__] = obj
    return wrappers

# ------------------------------------------------------------------
#  Fallback schema for tasks that declared no Pydantic schema
#  The body must look like:  { "params": { ... } }
# ------------------------------------------------------------------
class ParamWrapper(BaseModel):
    params: Dict[str, Any]

# ------------------------------------------------------------------
#  Build a FastAPI app that auto-wires ModelQ tasks
# ------------------------------------------------------------------
def create_api_app(modelq_instance):

    app = FastAPI(title="ModelQ Tasks API")

    # ---------- health ----------
    @app.get("/healthz")
    def healthz(): return {"status": "ok"}

    @app.get("/status")
    def status():
        return {
            "registered_servers": modelq_instance.get_registered_server_ids(),
            "queued_tasks_count": len(modelq_instance.get_all_queued_tasks()),
            "allowed_tasks": list(modelq_instance.allowed_tasks),
        }

    @app.get("/queue")
    def queue():
        return {"queued_tasks": modelq_instance.get_all_queued_tasks()}

    # ---------- task helpers ----------
    @app.get("/task/{task_id}/status")
    def get_task_status(task_id: str):
        st = modelq_instance.get_task_status(task_id)
        if st is None:
            raise HTTPException(404, detail="Task not found")
        return {"task_id": task_id, "status": st}

    @app.get("/task/{task_id}/result")
    def get_task_result(task_id: str):
        blob = modelq_instance.redis_client.get(f"task_result:{task_id}")
        if not blob:
            raise HTTPException(404, detail="Task not found or not completed yet")
        return json.loads(blob)

    # ---------- dynamic endpoints ----------
    wrapper_map = _discover_task_wrappers()

    for task_name in modelq_instance.allowed_tasks:

        task_func = wrapper_map.get(task_name) or getattr(modelq_instance, task_name)
        schema  = (getattr(task_func, "_mq_schema", None) or
                   getattr(getattr(task_func, "__wrapped__", None), "_mq_schema", None))
        returns = (getattr(task_func, "_mq_returns", None) or
                   getattr(getattr(task_func, "__wrapped__", None), "_mq_returns", None))

        # if no schema declared → use ParamWrapper contract
        if schema is None:
            schema = ParamWrapper

        endpoint_path = f"/task/{task_name}"

        # ---- factory with captured defaults (avoid late-binding) ----
        def make_endpoint(
            _func   = task_func,
            _schema = schema,
            _returns= returns,
            _tname  = task_name
        ):
            async def endpoint(payload: _schema, request: Request):  # type: ignore[valid-type]
                # ----- normalise call signature -----
                if isinstance(payload, ParamWrapper):
                    job = _func(**payload.params)
                elif isinstance(payload, dict):
                    job = _func(**payload)
                else:
                    job = _func(payload)

                try:
                    if job.stream:
                        return StreamingResponse(
                            job.get_stream(modelq_instance.redis_client), media_type="text/event-stream"
                        )
                    else:
                        result = job.get_result(
                            modelq_instance.redis_client,
                            timeout=3,
                            returns=_returns,
                            modelq_ref=modelq_instance,
                        )
                        if isinstance(result, BaseModel):
                            return JSONResponse(content=result.model_dump())
                        elif isinstance(result, dict):
                            return JSONResponse(content=result)
                        else:
                            return JSONResponse(content={"result": result})

                except TaskTimeoutError:
                    return JSONResponse(
                        status_code=202,
                        content={
                            "message": "Request is queued. Check status/result later.",
                            "task_id": job.task_id,
                            "status": "queued",
                        },
                    )
                except Exception as e:
                    raise HTTPException(400, detail=f"Error processing task {_tname}: {e}")

            return endpoint

        app.post(endpoint_path, response_model=returns or dict)(make_endpoint())

    return app

# ------------------------------------------------------------------
#  Typer CLI helper
# ------------------------------------------------------------------
def run_api(modelq_instance, host: str="0.0.0.0", port: int=8000):
    uvicorn.run(create_api_app(modelq_instance), host=host, port=port)
