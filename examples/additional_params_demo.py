"""
Additional Params Demo

This example demonstrates how to use the additional_params feature
to attach custom metadata to tasks that will be included in the task response.

Use case: When you need to track additional information with your tasks
like proxy URLs, public URLs, request IDs, tracking info, etc.
"""

import time
import redis
from modelq import ModelQ
from pydantic import BaseModel


# Initialize Redis and ModelQ
redis_client = redis.Redis(host="localhost", port=6379, db=0)
modelq_app = ModelQ(redis_client=redis_client)


# ---------------------------------------------------------------------------
# Example 1: Simple task with additional_params
# ---------------------------------------------------------------------------

@modelq_app.task()
def process_image(image_url: str, width: int, height: int):
    """Simulates image processing."""
    print(f"Processing image: {image_url} to size {width}x{height}")
    time.sleep(2)
    return f"Processed {image_url}"


# Call the task with additional_params
print("Example 1: Image processing with proxy and public links")
print("-" * 60)

task1 = process_image(
    "https://example.com/image.jpg",
    800,
    600,
    additional_params={
        "proxy_links": [
            "http://proxy1.example.com/image_processed.jpg",
            "http://proxy2.example.com/image_processed.jpg"
        ],
        "public_links": "http://cdn.example.com/image_processed.jpg",
        "request_id": "req_12345",
        "user_id": "user_789"
    }
)

print(f"Task created: {task1.task_id}")
print(f"Additional params: {task1.additional_params}")
print()

# Get task info immediately (while queued)
task_info = modelq_app.get_task_details(task1.task_id)
print("Task info (queued):")
print(f"  Task ID: {task_info['task_id']}")
print(f"  Status: {task_info['status']}")
print(f"  Proxy Links: {task_info.get('proxy_links', [])}")
print(f"  Public Links: {task_info.get('public_links', 'N/A')}")
print(f"  Request ID: {task_info.get('request_id', 'N/A')}")
print(f"  User ID: {task_info.get('user_id', 'N/A')}")
print()


# ---------------------------------------------------------------------------
# Example 2: Task with Pydantic schema and additional_params
# ---------------------------------------------------------------------------

class TextGenerationInput(BaseModel):
    prompt: str
    max_tokens: int
    temperature: float


class TextGenerationOutput(BaseModel):
    text: str
    tokens_used: int


@modelq_app.task(schema=TextGenerationInput, returns=TextGenerationOutput)
def generate_text(params: TextGenerationInput):
    """Simulates text generation."""
    print(f"Generating text for prompt: {params.prompt[:50]}...")
    time.sleep(1)
    return TextGenerationOutput(
        text=f"Generated text based on: {params.prompt}",
        tokens_used=100
    )


print("\nExample 2: Text generation with Pydantic schema and metadata")
print("-" * 60)

task2 = generate_text(
    TextGenerationInput(
        prompt="Write a story about a robot",
        max_tokens=500,
        temperature=0.7
    ),
    additional_params={
        "model_name": "gpt-4",
        "api_version": "v1",
        "cost_estimate": 0.002,
        "billing_id": "billing_xyz",
        "tags": ["generation", "story", "robot"]
    }
)

print(f"Task created: {task2.task_id}")
print(f"Additional params: {task2.additional_params}")
print()

task2_info = modelq_app.get_task_details(task2.task_id)
print("Task info (queued):")
print(f"  Task ID: {task2_info['task_id']}")
print(f"  Status: {task2_info['status']}")
print(f"  Model: {task2_info.get('model_name', 'N/A')}")
print(f"  API Version: {task2_info.get('api_version', 'N/A')}")
print(f"  Cost Estimate: ${task2_info.get('cost_estimate', 0)}")
print(f"  Tags: {task2_info.get('tags', [])}")
print()


# ---------------------------------------------------------------------------
# Example 3: Task without additional_params (backward compatibility)
# ---------------------------------------------------------------------------

@modelq_app.task()
def simple_calculation(a: int, b: int):
    """Simple task without additional params."""
    time.sleep(1)
    return a + b


print("\nExample 3: Simple task without additional_params")
print("-" * 60)

task3 = simple_calculation(10, 20)

print(f"Task created: {task3.task_id}")
print(f"Additional params: {task3.additional_params}")  # Should be empty dict
print()

task3_info = modelq_app.get_task_details(task3.task_id)
print("Task info (queued):")
print(f"  Task ID: {task3_info['task_id']}")
print(f"  Status: {task3_info['status']}")
print(f"  Has proxy_links: {'proxy_links' in task3_info}")
print(f"  Has public_links: {'public_links' in task3_info}")
print()


# ---------------------------------------------------------------------------
# Example 4: Complete workflow with worker
# ---------------------------------------------------------------------------

print("\nExample 4: Complete workflow with worker")
print("-" * 60)
print("Starting worker to process tasks...")
print("(In production, run worker in a separate process)")
print()

# Start worker in background (for demo purposes, run for limited time)
import threading


def run_worker_for_demo():
    """Run worker for a short time to process demo tasks."""
    modelq_app.start_workers(no_of_workers=2)
    # Worker will process tasks in background


# Start worker thread
worker_thread = threading.Thread(target=run_worker_for_demo, daemon=True)
worker_thread.start()

# Give worker time to start
time.sleep(1)

# Create a task to process
print("Creating img2img task with additional params...")
task4 = process_image(
    "https://example.com/input.jpg",
    1024,
    1024,
    additional_params={
        "proxy_links": ["http://proxy.cdn.com/output.jpg"],
        "public_links": "http://cdn.example.com/output.jpg",
        "webhook_url": "https://api.example.com/webhook",
        "priority": "high",
        "user_metadata": {
            "user_id": "user_123",
            "session_id": "sess_456"
        }
    }
)

print(f"Task ID: {task4.task_id}")
print()

# Wait for task to complete
print("Waiting for task to complete...")
try:
    result = task4.get_result(redis_client, timeout=10)
    print(f"Result: {result}")
    print()

    # Get final task details
    final_info = modelq_app.get_task_details(task4.task_id)
    print("Final task info:")
    print(f"  Status: {final_info['status']}")
    print(f"  Result: {final_info.get('result', 'N/A')}")
    print(f"  Proxy Links: {final_info.get('proxy_links', [])}")
    print(f"  Public Links: {final_info.get('public_links', 'N/A')}")
    print(f"  Webhook URL: {final_info.get('webhook_url', 'N/A')}")
    print(f"  Priority: {final_info.get('priority', 'N/A')}")
    print(f"  User Metadata: {final_info.get('user_metadata', {})}")
    print()

except Exception as e:
    print(f"Error: {e}")


# ---------------------------------------------------------------------------
# Example 5: API response format
# ---------------------------------------------------------------------------

print("\nExample 5: Simulated API response format")
print("-" * 60)
print("This shows how your task response would look in an API:")
print()

# Create a task
api_task = process_image(
    "https://example.com/api_image.jpg",
    512,
    512,
    additional_params={
        "proxy_links": [
            "http://proxy1.example.com/result.jpg",
            "http://proxy2.example.com/result.jpg"
        ],
        "public_links": "http://cdn.example.com/result.jpg"
    }
)

# Simulate API response
import json

api_response = api_task.to_dict()
print(json.dumps(api_response, indent=2))
print()

print("=" * 60)
print("Demo complete!")
print()
print("Key takeaways:")
print("1. Use additional_params to attach custom metadata to tasks")
print("2. Additional params appear at the root level of task responses")
print("3. Works with both regular tasks and Pydantic-validated tasks")
print("4. Completely optional - tasks work normally without it")
print("5. Perfect for tracking proxy URLs, public URLs, request IDs, etc.")
