from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import time
import json
import gc
from typing import Dict, Callable, Any

# ------------------------------------------------------------
# Helper – find every *wrapper* produced by @mq.task anywhere
# in the current Python process.
# A wrapper will have __wrapped__ (thanks to functools.wraps)
# and the *inner* function carries the _mq_schema attribute.
# ------------------------------------------------------------

def _discover_task_wrappers() -> Dict[str, Callable[..., Any]]:
    wrappers: Dict[str, Callable[..., Any]] = {}
    for obj in gc.get_objects():
        # Must be callable and look like a functools.wraps wrapper
        if callable(obj) and hasattr(obj, "__wrapped__"):
            inner = getattr(obj, "__wrapped__", None)
            if inner and hasattr(inner, "_mq_schema"):
                wrappers[inner.__name__] = obj  # key = task name
    return wrappers

# ------------------------------------------------------------
# Factory that builds a FastAPI app wired to ModelQ automatically
# ------------------------------------------------------------

def create_api_app(modelq_instance):
    app = FastAPI(title="ModelQ Tasks API")

    # ---------- Health ----------
    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/status")
    def status():
        servers = modelq_instance.get_registered_server_ids()
        queued = modelq_instance.get_all_queued_tasks()
        return {
            "registered_servers": servers,
            "queued_tasks_count": len(queued),
            "allowed_tasks": list(modelq_instance.allowed_tasks),
        }

    @app.get("/queue")
    def queue():
        return {"queued_tasks": modelq_instance.get_all_queued_tasks()}

    # ---------- Task‑level helpers ----------
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

    # --------------------------------------------------------
    # Dynamic endpoints: use wrapper if we discovered one.
    # Fallback to ModelQ attribute (original) if wrapper missing.
    # --------------------------------------------------------
    wrapper_map = _discover_task_wrappers()

    for task_name in modelq_instance.allowed_tasks:
        task_func = wrapper_map.get(task_name) or getattr(modelq_instance, task_name)
        schema  = getattr(task_func, "_mq_schema",  None) or getattr(getattr(task_func, "__wrapped__", None), "_mq_schema", None)
        returns = getattr(task_func, "_mq_returns", None) or getattr(getattr(task_func, "__wrapped__", None), "_mq_returns", None)
        endpoint_path = f"/task/{task_name}"

        # Closure-bound defaults to avoid late‑binding bugs
        def make_endpoint(_func=task_func, _schema=schema, _returns=returns, _tname=task_name):
            async def endpoint(payload: _schema, request: Request):  # type: ignore[valid-type]
                job = None
                try:
                    # Call wrapper → returns Task; call original → might return AddOut
                    job = _func(payload)
                    # Try quick result (3 s)
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
                except Exception as e:
                    if "timeout" in str(e).lower() or isinstance(e, TimeoutError):
                        return JSONResponse(
                            status_code=202,
                            content={
                                "message": "Request is queued. Check status/result later.",
                                "task_id": getattr(job, "task_id", "unknown"),
                                "status": "queued",
                            },
                        )
                    raise HTTPException(400, detail=str(e))

            return endpoint

        # Pydantic schema may be None → accept empty body
        if schema is not None:
            app.post(endpoint_path, response_model=returns or dict)(make_endpoint())
        else:
            class AnyInput(BaseModel):
                pass
            app.post(endpoint_path, response_model=returns or dict)(make_endpoint(_schema=AnyInput))

    return app

# ------------------------------------------------------------
# Entry helper used by Typer CLI
# ------------------------------------------------------------

def run_api(modelq_instance, host="0.0.0.0", port=8000):
    """Spin up the auto‑wired FastAPI server."""
    app = create_api_app(modelq_instance)
    uvicorn.run(app, host=host, port=port)