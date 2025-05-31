import typer
import importlib
import os
import sys
import logging
import time
import signal
import threading
from typing import Optional
from modelq.app.api.server import run_api

app = typer.Typer(help="ModelQ CLI for managing and queuing tasks.")

# Global variable to handle graceful shutdown
shutdown_event = threading.Event()

def setup_logging(log_level: str = "INFO"):
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("modelq.log")
        ]
    )

def signal_handler(signum, frame):
    print(f"\n🛑 Received signal {signum}. Shutting down gracefully...")
    shutdown_event.set()

def load_app_instance(app_path: str):
    if ":" not in app_path:
        typer.echo("❌ Format should be module:object (e.g., 'myapp:modelq_instance')")
        raise typer.Exit(1)

    module_name, var_name = app_path.split(":", 1)

    sys.path.insert(0, os.getcwd())

    try:
        mod = importlib.import_module(module_name)
        app_instance = getattr(mod, var_name)
        if not hasattr(app_instance, 'start_workers'):
            typer.echo(f"❌ {var_name} is not a valid ModelQ instance")
            raise typer.Exit(1)
        return app_instance
    except Exception as e:
        typer.echo(f"❌ Failed to load app instance: {e}")
        raise typer.Exit(1)

@app.command()
def version():
    typer.echo("ModelQ v0.1.0")

@app.command()
def run_workers(
    app_path: str,
    workers: int = typer.Option(1, "--workers", "-w", help="Number of worker threads"),
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Logging level"),
    log_file: Optional[str] = typer.Option(None, "--log-file", "-f", help="Log file path")
):
    setup_logging(log_level)
    logger = logging.getLogger("modelq.cli")

    app_instance = load_app_instance(app_path)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    typer.echo(f"🚀 Starting ModelQ workers...")
    typer.echo(f"   Workers: {workers}")
    typer.echo(f"   Log Level: {log_level}")
    typer.echo(f"   Redis Host: {getattr(app_instance.redis_client.connection_pool, 'connection_kwargs', {}).get('host', 'unknown')}")
    typer.echo(f"   Registered Tasks: {', '.join(app_instance.allowed_tasks) if app_instance.allowed_tasks else 'None'}")
    typer.echo("   Press Ctrl+C to stop")
    typer.echo("-" * 50)

    try:
        logger.info(f"Starting {workers} worker(s)")
        app_instance.start_workers(no_of_workers=workers)
        typer.echo("✅ Workers are running. Waiting for tasks...")
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Error running workers: {e}")
        typer.echo(f"❌ Error: {e}")
        raise typer.Exit(1)
    finally:
        logger.info("Shutting down workers...")
        typer.echo("🛑 Shutting down workers...")
        typer.echo("✅ Shutdown complete")

@app.command()
def status(app_path: str):
    app_instance = load_app_instance(app_path)

    try:
        servers = app_instance.get_registered_server_ids()
        queued_tasks = app_instance.get_all_queued_tasks()

        typer.echo("📊 ModelQ Status:")
        typer.echo(f"   Registered Servers: {len(servers)}")
        typer.echo(f"   Queued Tasks: {len(queued_tasks)}")
        typer.echo(f"   Allowed Tasks: {', '.join(app_instance.allowed_tasks) if app_instance.allowed_tasks else 'None'}")

        if servers:
            typer.echo("\n🖥️  Active Servers:")
            for server_id in servers:
                typer.echo(f"   - {server_id}")

    except Exception as e:
        typer.echo(f"❌ Failed to get status: {e}")
        raise typer.Exit(1)

@app.command()
def clear_queue(app_path: str):
    """Clear all tasks from the ML queue."""
    app_instance = load_app_instance(app_path)
    try:
        app_instance.delete_queue()
        typer.echo("🗑️  Cleared all tasks from the queue.")
    except Exception as e:
        typer.echo(f"❌ Failed to clear queue: {e}")
        raise typer.Exit(1)

@app.command()
def remove_task(app_path: str, task_id: str):
    """Remove a task from the queue by task ID."""
    app_instance = load_app_instance(app_path)
    try:
        removed = app_instance.remove_task_from_queue(task_id)
        if removed:
            typer.echo(f"🗑️  Task {task_id} removed from queue.")
        else:
            typer.echo(f"⚠️  Task {task_id} not found in queue.")
    except Exception as e:
        typer.echo(f"❌ Failed to remove task: {e}")
        raise typer.Exit(1)

@app.command()
def list_queued(app_path: str):
    """List all currently queued tasks."""
    app_instance = load_app_instance(app_path)
    try:
        tasks = app_instance.get_all_queued_tasks()
        if not tasks:
            typer.echo("📭 No tasks in queue.")
            return

        typer.echo(f"📋 Queued Tasks ({len(tasks)}):")
        for t in tasks:
            typer.echo(f" - {t['task_id']} ({t['task_name']})")
    except Exception as e:
        typer.echo(f"❌ Failed to list queued tasks: {e}")
        raise typer.Exit(1)

@app.command("serve-api")
def serve_api_cmd(
    app_path: str,
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host to bind the API server"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to serve the API"),
    log_level: str = typer.Option("info", "--log-level", "-l", help="Uvicorn/FastAPI log level")
):
    app_instance = load_app_instance(app_path)
    typer.echo(f"🌐 Starting ModelQ API server on http://{host}:{port} ...")
    typer.echo(f"   Registered Tasks: {', '.join(app_instance.allowed_tasks) if app_instance.allowed_tasks else 'None'}")
    run_api(app_instance, host=host, port=port)

if __name__ == "__main__":
    app()
