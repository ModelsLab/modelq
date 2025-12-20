"""
ModelQ - Task History Example

This example demonstrates how to:
- View past tasks and their status
- Get error details from failed tasks
- Monitor remote worker activity
- Get task statistics
"""

from datetime import datetime
from modelq import ModelQ
from redis import Redis

print("=== ModelQ Task History Example ===\n")

redis_client = Redis(host="localhost", port=6379, db=0)
mq = ModelQ(redis_client=redis_client)


# Register tasks
@mq.task()
def process_image(params: dict):
    url = params.get("url")
    # Simulate image processing
    return {"status": "processed", "url": url}


@mq.task()
def generate_text(params: dict):
    prompt = params.get("prompt")
    return {"text": f"Generated from: {prompt}"}


@mq.task()
def failing_task(params: dict):
    # This task will fail
    raise ValueError("Simulated task failure!")


# ============================================
# 1. Get Task Details (including errors)
# ============================================
print("1. Get Task Details")

task = process_image({"url": "https://example.com/image.jpg"})
print(f"   Enqueued task: {task.task_id}")

details = mq.get_task_details(task.task_id)
if details:
    print(f"   Task Name: {details['task_name']}")
    print(f"   Status: {details['status']}")
    if 'created_at' in details:
        created = datetime.fromtimestamp(details['created_at']).strftime('%Y-%m-%d %H:%M:%S')
        print(f"   Created: {created}")

    # If task failed, error details are available:
    if 'error' in details:
        print(f"   Error Type: {details['error']['type']}")
        print(f"   Error Message: {details['error']['message']}")
        if details['error'].get('file'):
            print(f"   Error File: {details['error']['file']}:{details['error']['line']}")
print()

# ============================================
# 2. Get Task History
# ============================================
print("2. Get Task History (most recent first)")

# Add a few more tasks for demonstration
process_image({"url": "image1.jpg"})
generate_text({"prompt": "Hello"})
process_image({"url": "image2.jpg"})

history = mq.get_task_history(limit=10)
print("   Recent tasks:")
for task_data in history:
    created = task_data.get('created_at')
    time_str = datetime.fromtimestamp(created).strftime('%H:%M:%S') if created else 'N/A'
    print(f"   - [{time_str}] {task_data['task_name']} ({task_data['status']})")
print()

# ============================================
# 3. Filter Tasks by Status
# ============================================
print("3. Filter Tasks by Status")

queued_tasks = mq.get_task_history(limit=10, status='queued')
print(f"   Queued tasks: {len(queued_tasks)}")

completed_tasks = mq.get_completed_tasks(limit=10)
print(f"   Completed tasks: {len(completed_tasks)}")

failed_tasks = mq.get_failed_tasks(limit=10)
print(f"   Failed tasks: {len(failed_tasks)}")

# Show failed task errors
if failed_tasks:
    print("\n   Failed task details:")
    for failed in failed_tasks:
        print(f"   - Task: {failed['task_id']}")
        print(f"     Name: {failed['task_name']}")
        if 'error' in failed:
            print(f"     Error: {failed['error']['message']}")
            print(f"     Type: {failed['error']['type']}")
print()

# ============================================
# 4. Filter Tasks by Name
# ============================================
print("4. Filter Tasks by Name")

image_tasks = mq.get_tasks_by_name('process_image', limit=10)
print(f"   process_image tasks: {len(image_tasks)}")

text_tasks = mq.get_tasks_by_name('generate_text', limit=10)
print(f"   generate_text tasks: {len(text_tasks)}")
print()

# ============================================
# 5. Get Task Statistics
# ============================================
print("5. Task Statistics")

stats = mq.get_task_stats()
print(f"   Total tasks: {stats['total']}")
print("\n   By Status:")
for status, count in stats['by_status'].items():
    print(f"     {status}: {count}")

print("\n   By Task Name:")
for name, counts in stats['by_task_name'].items():
    print(f"     {name}: {counts['total']} total, {counts['completed']} completed, {counts['failed']} failed")

if stats['failed_tasks']:
    print("\n   Recent Errors:")
    for failed in stats['failed_tasks']:
        print(f"     - {failed['task_name']}: {failed['error']}")
print()

# ============================================
# 6. Task History Count
# ============================================
print("6. Task History Count")
count = mq.get_task_history_count()
print(f"   Total tasks in history: {count}\n")

# ============================================
# 7. Clear Old History
# ============================================
print("7. Clear Old History")
# Clear tasks older than 7 days (default)
# removed = mq.clear_task_history()

# Clear tasks older than 1 hour
# removed = mq.clear_task_history(3600)

print("   (Skipped - run manually to clean up)")
print("   Usage: mq.clear_task_history(3600)  # older than 1 hour\n")

# ============================================
# 8. Monitoring Remote Workers (Code Example)
# ============================================
print("8. Monitoring Remote Workers\n")

monitoring_code = '''
# Dashboard/Admin Controller Example

class TaskDashboard:
    def __init__(self, modelq: ModelQ):
        self.modelq = modelq

    def index(self):
        return {
            'stats': self.modelq.get_task_stats(),
            'recent_failed': self.modelq.get_failed_tasks(limit=20),
            'history_count': self.modelq.get_task_history_count(),
            'servers': self.modelq.get_registered_server_ids(),
        }

    def task_details(self, task_id: str):
        details = self.modelq.get_task_details(task_id)

        if not details:
            raise Exception('Task not found')

        return details  # Includes error info if failed

    def failed_tasks(self):
        failed = self.modelq.get_failed_tasks(limit=100)

        return [
            {
                'id': task['task_id'],
                'name': task['task_name'],
                'error': task.get('error', {}).get('message', 'Unknown'),
                'error_type': task.get('error', {}).get('type'),
                'failed_at': task.get('finished_at'),
            }
            for task in failed
        ]
'''

print(monitoring_code)

# Cleanup for demo
mq.delete_queue()
mq.clear_task_history(0)  # Clear all

print("\n=== Task History Example Complete ===")
