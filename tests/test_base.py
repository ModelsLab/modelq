import json
import time
import fakeredis
import pytest

from modelq import ModelQ

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _json_bytes_to_dict(blob):
    """Decode Redis bytes → dict."""
    return json.loads(blob.decode() if isinstance(blob, (bytes, bytearray)) else blob)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_redis():
    return fakeredis.FakeStrictRedis()

@pytest.fixture
def modelq_instance(mock_redis):
    # No extra kwargs – defaults on the new ModelQ are fine
    return ModelQ(redis_client=mock_redis)


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_register_server(modelq_instance):
    # register_server is already called in __init__, but calling again is harmless
    modelq_instance.register_server()
    raw = modelq_instance.redis_client.hget("servers", modelq_instance.server_id)
    assert raw is not None
    data = _json_bytes_to_dict(raw)
    assert data["status"] == "idle"
    assert isinstance(data["last_heartbeat"], float)


def test_modelq_initialization(modelq_instance):
    assert modelq_instance.server_id
    assert modelq_instance.redis_client


def test_enqueue_task(modelq_instance):
    task_data = {"task_id": "123", "status": "new"}
    payload = {"data": "sample"}

    modelq_instance.enqueue_task(task_data, payload)

    # queued in list ---------------------------------------------------------
    queued_blob = modelq_instance.redis_client.lpop("ml_tasks")
    assert queued_blob
    queued = _json_bytes_to_dict(queued_blob)
    assert queued["task_id"] == "123"
    assert queued["status"] == "queued"
    assert "queued_at" in queued

    # registered in sorted-set -----------------------------------------------
    assert modelq_instance.redis_client.zscore("queued_requests", "123") is not None


def test_requeue_stuck_processing_tasks(modelq_instance):
    task_id = "stuck_task"
    task_data = {
        "task_id": task_id,
        "status": "processing",
        "started_at": time.time() - 200,
    }
    # simulate a stuck task
    modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_data))
    modelq_instance.redis_client.sadd("processing_tasks", task_id)

    modelq_instance.requeue_stuck_processing_tasks(threshold=180)

    queued_blob = modelq_instance.redis_client.lpop("ml_tasks")
    assert queued_blob
    queued = _json_bytes_to_dict(queued_blob)
    assert queued["task_id"] == task_id
    assert queued["status"] == "queued"
    assert "queued_at" in queued

    # processing set cleaned up
    assert task_id.encode() not in modelq_instance.redis_client.smembers("processing_tasks")


def test_prune_old_task_results(modelq_instance):
    old_id = "old_task"
    old_task_data = {
        "task_id": old_id,
        "status": "completed",
        "finished_at": time.time() - 90_000,
    }
    modelq_instance.redis_client.set(f"task_result:{old_id}", json.dumps(old_task_data))
    modelq_instance.redis_client.set(f"task:{old_id}", json.dumps(old_task_data))

    modelq_instance.prune_old_task_results(older_than_seconds=86_400)

    assert modelq_instance.redis_client.get(f"task_result:{old_id}") is None
    assert modelq_instance.redis_client.get(f"task:{old_id}") is None


def test_heartbeat(modelq_instance):
    modelq_instance.register_server()
    initial = _json_bytes_to_dict(
        modelq_instance.redis_client.hget("servers", modelq_instance.server_id)
    )["last_heartbeat"]

    time.sleep(1)
    modelq_instance.heartbeat()

    updated = _json_bytes_to_dict(
        modelq_instance.redis_client.hget("servers", modelq_instance.server_id)
    )["last_heartbeat"]

    assert updated > initial


def test_enqueue_and_retrieve_task(modelq_instance):
    task_data = {"task_id": "task_456", "status": "new"}
    payload = {"data": "sample"}

    modelq_instance.enqueue_task(task_data, payload)
    queued = _json_bytes_to_dict(modelq_instance.redis_client.lpop("ml_tasks"))

    assert queued["task_id"] == "task_456"
    assert queued["status"] == "queued"
    assert "queued_at" in queued


def test_enqueue_delayed_task(modelq_instance):
    task_dict = {"task_id": "delayed_task", "status": "new"}
    delay = 10

    modelq_instance.enqueue_delayed_task(task_dict, delay)

    # should exist in delayed_tasks with a score ~ now + delay
    now_plus_delay = time.time() + delay
    delayed = modelq_instance.redis_client.zrangebyscore("delayed_tasks", 0, now_plus_delay)
    assert len(delayed) == 1
    assert _json_bytes_to_dict(delayed[0])["task_id"] == "delayed_task"


def test_get_all_queued_tasks(modelq_instance):
    # clean slate
    modelq_instance.delete_queue()

    tasks = [
        {"task_id": "task_1", "status": "queued"},
        {"task_id": "task_2", "status": "queued"},
    ]
    for t in tasks:
        modelq_instance.redis_client.rpush("ml_tasks", json.dumps(t))

    queued = modelq_instance.get_all_queued_tasks()
    assert [t["task_id"] for t in queued] == ["task_1", "task_2"]


def test_get_registered_server_ids(modelq_instance):
    modelq_instance.register_server()
    server_ids = modelq_instance.get_registered_server_ids()
    assert modelq_instance.server_id in server_ids


# ---------------------------------------------------------------------------
# Task History Tests
# ---------------------------------------------------------------------------

def test_add_to_task_history(modelq_instance):
    """Test that tasks are added to history when enqueued."""
    task_data = {"task_id": "history_task_1", "status": "new", "task_name": "test_task"}
    payload = {"data": "sample"}

    modelq_instance.enqueue_task(task_data, payload)

    # Task should be in history sorted set
    score = modelq_instance.redis_client.zscore("task_history", "history_task_1")
    assert score is not None

    # Task data should be stored
    history_data = modelq_instance.redis_client.get("task_history:history_task_1")
    assert history_data is not None
    task_dict = _json_bytes_to_dict(history_data)
    assert task_dict["task_id"] == "history_task_1"
    assert task_dict["status"] == "queued"


def test_get_task_details(modelq_instance):
    """Test getting task details from history."""
    task_data = {
        "task_id": "detail_task_1",
        "task_name": "process_image",
        "status": "queued",
        "created_at": time.time()
    }
    modelq_instance.redis_client.set(
        "task_history:detail_task_1",
        json.dumps(task_data)
    )
    modelq_instance.redis_client.zadd("task_history", {"detail_task_1": task_data["created_at"]})

    details = modelq_instance.get_task_details("detail_task_1")
    assert details is not None
    assert details["task_id"] == "detail_task_1"
    assert details["task_name"] == "process_image"
    assert details["status"] == "queued"


def test_get_task_details_not_found(modelq_instance):
    """Test getting details for non-existent task."""
    details = modelq_instance.get_task_details("non_existent_task")
    assert details is None


def test_get_task_history(modelq_instance):
    """Test getting task history with multiple tasks."""
    now = time.time()
    tasks = [
        {"task_id": "hist_1", "task_name": "task_a", "status": "completed", "created_at": now - 100},
        {"task_id": "hist_2", "task_name": "task_b", "status": "failed", "created_at": now - 50},
        {"task_id": "hist_3", "task_name": "task_a", "status": "queued", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    history = modelq_instance.get_task_history(limit=10)
    assert len(history) == 3
    # Most recent first
    assert history[0]["task_id"] == "hist_3"
    assert history[1]["task_id"] == "hist_2"
    assert history[2]["task_id"] == "hist_1"


def test_get_task_history_with_status_filter(modelq_instance):
    """Test filtering task history by status."""
    now = time.time()
    tasks = [
        {"task_id": "filter_1", "task_name": "task_a", "status": "completed", "created_at": now - 100},
        {"task_id": "filter_2", "task_name": "task_b", "status": "failed", "created_at": now - 50},
        {"task_id": "filter_3", "task_name": "task_a", "status": "completed", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    completed = modelq_instance.get_task_history(status="completed")
    assert len(completed) == 2
    assert all(t["status"] == "completed" for t in completed)

    failed = modelq_instance.get_task_history(status="failed")
    assert len(failed) == 1
    assert failed[0]["task_id"] == "filter_2"


def test_get_task_history_with_name_filter(modelq_instance):
    """Test filtering task history by task name."""
    now = time.time()
    tasks = [
        {"task_id": "name_1", "task_name": "process_image", "status": "completed", "created_at": now - 100},
        {"task_id": "name_2", "task_name": "generate_text", "status": "completed", "created_at": now - 50},
        {"task_id": "name_3", "task_name": "process_image", "status": "completed", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    image_tasks = modelq_instance.get_task_history(task_name="process_image")
    assert len(image_tasks) == 2
    assert all(t["task_name"] == "process_image" for t in image_tasks)


def test_get_failed_tasks(modelq_instance):
    """Test convenience method for getting failed tasks."""
    now = time.time()
    tasks = [
        {"task_id": "fail_1", "task_name": "task_a", "status": "failed", "created_at": now - 100},
        {"task_id": "fail_2", "task_name": "task_b", "status": "completed", "created_at": now - 50},
        {"task_id": "fail_3", "task_name": "task_a", "status": "failed", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    failed = modelq_instance.get_failed_tasks()
    assert len(failed) == 2
    assert all(t["status"] == "failed" for t in failed)


def test_get_completed_tasks(modelq_instance):
    """Test convenience method for getting completed tasks."""
    now = time.time()
    tasks = [
        {"task_id": "comp_1", "task_name": "task_a", "status": "completed", "created_at": now - 100},
        {"task_id": "comp_2", "task_name": "task_b", "status": "failed", "created_at": now - 50},
        {"task_id": "comp_3", "task_name": "task_a", "status": "completed", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    completed = modelq_instance.get_completed_tasks()
    assert len(completed) == 2
    assert all(t["status"] == "completed" for t in completed)


def test_get_tasks_by_name(modelq_instance):
    """Test getting tasks filtered by name."""
    now = time.time()
    tasks = [
        {"task_id": "byname_1", "task_name": "process_image", "status": "completed", "created_at": now - 100},
        {"task_id": "byname_2", "task_name": "generate_text", "status": "completed", "created_at": now - 50},
        {"task_id": "byname_3", "task_name": "process_image", "status": "failed", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    image_tasks = modelq_instance.get_tasks_by_name("process_image")
    assert len(image_tasks) == 2
    assert all(t["task_name"] == "process_image" for t in image_tasks)


def test_get_task_stats(modelq_instance):
    """Test getting task statistics."""
    now = time.time()
    tasks = [
        {"task_id": "stats_1", "task_name": "process_image", "status": "completed", "created_at": now - 100},
        {"task_id": "stats_2", "task_name": "process_image", "status": "failed", "created_at": now - 50,
         "error": {"message": "Test error"}},
        {"task_id": "stats_3", "task_name": "generate_text", "status": "completed", "created_at": now},
        {"task_id": "stats_4", "task_name": "process_image", "status": "queued", "created_at": now + 10},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    stats = modelq_instance.get_task_stats()
    assert stats["total"] == 4
    assert stats["by_status"]["completed"] == 2
    assert stats["by_status"]["failed"] == 1
    assert stats["by_status"]["queued"] == 1
    assert stats["by_task_name"]["process_image"]["total"] == 3
    assert stats["by_task_name"]["process_image"]["completed"] == 1
    assert stats["by_task_name"]["process_image"]["failed"] == 1
    assert stats["by_task_name"]["generate_text"]["total"] == 1
    assert len(stats["failed_tasks"]) == 1
    assert stats["failed_tasks"][0]["error"] == "Test error"


def test_get_task_history_count(modelq_instance):
    """Test counting tasks in history."""
    now = time.time()
    for i in range(5):
        task = {"task_id": f"count_{i}", "task_name": "test", "status": "completed", "created_at": now + i}
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    count = modelq_instance.get_task_history_count()
    assert count == 5


def test_clear_task_history(modelq_instance):
    """Test clearing old task history."""
    now = time.time()
    tasks = [
        {"task_id": "clear_1", "task_name": "test", "status": "completed", "created_at": now - 1000},
        {"task_id": "clear_2", "task_name": "test", "status": "completed", "created_at": now - 500},
        {"task_id": "clear_3", "task_name": "test", "status": "completed", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    # Clear tasks older than 600 seconds
    removed = modelq_instance.clear_task_history(600)
    assert removed == 1  # Only clear_1 should be removed

    # Verify remaining
    assert modelq_instance.redis_client.get("task_history:clear_1") is None
    assert modelq_instance.redis_client.get("task_history:clear_2") is not None
    assert modelq_instance.redis_client.get("task_history:clear_3") is not None


def test_task_history_with_error_details(modelq_instance):
    """Test that error details are properly stored in history."""
    task_data = {
        "task_id": "error_task_1",
        "task_name": "failing_task",
        "status": "failed",
        "created_at": time.time(),
        "error": {
            "message": "Division by zero",
            "type": "ZeroDivisionError",
            "file": "/app/tasks.py",
            "line": 42,
            "trace": "Traceback..."
        }
    }

    modelq_instance.redis_client.set(f"task_history:{task_data['task_id']}", json.dumps(task_data))
    modelq_instance.redis_client.zadd("task_history", {task_data['task_id']: task_data['created_at']})

    details = modelq_instance.get_task_details("error_task_1")
    assert details is not None
    assert "error" in details
    assert details["error"]["message"] == "Division by zero"
    assert details["error"]["type"] == "ZeroDivisionError"
    assert details["error"]["file"] == "/app/tasks.py"
    assert details["error"]["line"] == 42


def test_get_task_history_with_limit_and_offset(modelq_instance):
    """Test pagination with limit and offset."""
    now = time.time()
    for i in range(10):
        task = {"task_id": f"page_{i}", "task_name": "test", "status": "completed", "created_at": now + i}
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    # Get first 3
    page1 = modelq_instance.get_task_history(limit=3, offset=0)
    assert len(page1) == 3
    assert page1[0]["task_id"] == "page_9"  # Most recent first

    # Get next 3
    page2 = modelq_instance.get_task_history(limit=3, offset=3)
    assert len(page2) == 3
    assert page2[0]["task_id"] == "page_6"


# ---------------------------------------------------------------------------
# Task Cancellation Tests
# ---------------------------------------------------------------------------

def test_cancel_task(modelq_instance):
    """Test cancelling a task."""
    task_data = {
        "task_id": "cancel_test_1",
        "task_name": "test_task",
        "status": "queued",
        "created_at": time.time()
    }

    # Store task
    modelq_instance.redis_client.set(f"task:{task_data['task_id']}", json.dumps(task_data))
    modelq_instance.redis_client.rpush("ml_tasks", json.dumps(task_data))
    modelq_instance.redis_client.zadd("queued_requests", {task_data['task_id']: time.time()})

    # Cancel the task
    result = modelq_instance.cancel_task("cancel_test_1")
    assert result is True

    # Check cancellation flag is set
    assert modelq_instance.is_task_cancelled("cancel_test_1") is True

    # Check task status is updated
    updated_task = json.loads(modelq_instance.redis_client.get("task:cancel_test_1"))
    assert updated_task["status"] == "cancelled"


def test_cancel_nonexistent_task(modelq_instance):
    """Test cancelling a task that doesn't exist."""
    result = modelq_instance.cancel_task("nonexistent_task_id")
    assert result is False


def test_is_task_cancelled(modelq_instance):
    """Test checking if a task is cancelled."""
    # Not cancelled
    assert modelq_instance.is_task_cancelled("uncancelled_task") is False

    # Set cancellation flag
    modelq_instance.redis_client.set("task:cancelled_task:cancelled", "1")
    assert modelq_instance.is_task_cancelled("cancelled_task") is True


def test_get_cancelled_tasks(modelq_instance):
    """Test getting list of cancelled tasks."""
    now = time.time()
    tasks = [
        {"task_id": "cancelled_1", "task_name": "test", "status": "cancelled", "created_at": now - 100},
        {"task_id": "cancelled_2", "task_name": "test", "status": "completed", "created_at": now - 50},
        {"task_id": "cancelled_3", "task_name": "test", "status": "cancelled", "created_at": now},
    ]

    for task in tasks:
        modelq_instance.redis_client.set(f"task_history:{task['task_id']}", json.dumps(task))
        modelq_instance.redis_client.zadd("task_history", {task['task_id']: task['created_at']})

    cancelled = modelq_instance.get_cancelled_tasks()
    assert len(cancelled) == 2
    assert all(t["status"] == "cancelled" for t in cancelled)


# ---------------------------------------------------------------------------
# Progress Tracking Tests
# ---------------------------------------------------------------------------

def test_report_progress(modelq_instance):
    """Test reporting task progress."""
    task_id = "progress_test_1"

    # Report progress
    modelq_instance.report_progress(task_id, 0.5, "Halfway done")

    # Verify progress is stored
    progress_data = modelq_instance.redis_client.get(f"task:{task_id}:progress")
    assert progress_data is not None

    progress = json.loads(progress_data)
    assert progress["progress"] == 0.5
    assert progress["message"] == "Halfway done"
    assert "updated_at" in progress


def test_report_progress_clamps_values(modelq_instance):
    """Test that progress is clamped between 0 and 1."""
    task_id = "progress_clamp_test"

    # Test clamping above 1
    modelq_instance.report_progress(task_id, 1.5, "Over 100%")
    progress = json.loads(modelq_instance.redis_client.get(f"task:{task_id}:progress"))
    assert progress["progress"] == 1.0

    # Test clamping below 0
    modelq_instance.report_progress(task_id, -0.5, "Negative")
    progress = json.loads(modelq_instance.redis_client.get(f"task:{task_id}:progress"))
    assert progress["progress"] == 0.0


def test_get_task_progress(modelq_instance):
    """Test getting task progress."""
    task_id = "get_progress_test"

    # No progress yet
    assert modelq_instance.get_task_progress(task_id) is None

    # Report progress
    modelq_instance.report_progress(task_id, 0.75, "Almost done")

    # Get progress
    progress = modelq_instance.get_task_progress(task_id)
    assert progress is not None
    assert progress["progress"] == 0.75
    assert progress["message"] == "Almost done"


def test_report_progress_without_message(modelq_instance):
    """Test reporting progress without a message."""
    task_id = "progress_no_msg_test"

    modelq_instance.report_progress(task_id, 0.25)

    progress = modelq_instance.get_task_progress(task_id)
    assert progress["progress"] == 0.25
    assert progress["message"] is None


# ---------------------------------------------------------------------------
# Task TTL and Cleanup Tests
# ---------------------------------------------------------------------------

def test_cleanup_expired_tasks(modelq_instance):
    """Test cleaning up expired tasks from queue."""
    now = time.time()

    # Create an old task (expired)
    old_task = {
        "task_id": "expired_task",
        "task_name": "test",
        "status": "queued",
        "created_at": now - 100000  # Very old
    }
    modelq_instance.redis_client.rpush("ml_tasks", json.dumps(old_task))

    # Create a fresh task
    fresh_task = {
        "task_id": "fresh_task",
        "task_name": "test",
        "status": "queued",
        "created_at": now
    }
    modelq_instance.redis_client.rpush("ml_tasks", json.dumps(fresh_task))

    # Cleanup expired tasks
    removed = modelq_instance.cleanup_expired_tasks()
    assert removed == 1

    # Verify fresh task is still in queue
    queued = modelq_instance.get_all_queued_tasks()
    task_ids = [t["task_id"] for t in queued]
    assert "fresh_task" in task_ids
    assert "expired_task" not in task_ids


def test_configurable_task_ttl(mock_redis):
    """Test that task TTL is configurable."""
    # Custom TTL of 1 hour
    mq = ModelQ(redis_client=mock_redis, task_ttl=3600)
    assert mq.task_ttl == 3600


def test_configurable_history_retention(mock_redis):
    """Test that history retention is configurable."""
    # Custom retention of 1 hour
    mq = ModelQ(redis_client=mock_redis, task_history_retention=3600)
    assert mq.task_history_retention == 3600


# ---------------------------------------------------------------------------
# Additional Params Tests
# ---------------------------------------------------------------------------

def test_task_with_additional_params(modelq_instance):
    """Test that tasks can include additional custom parameters."""

    @modelq_instance.task()
    def test_task_with_params(x, y):
        return x + y

    # Call task with additional_params
    task = test_task_with_params(
        5, 10,
        additional_params={
            "proxy_links": ["http://proxy1.example.com", "http://proxy2.example.com"],
            "public_links": "http://public.example.com",
            "custom_field": "custom_value"
        }
    )

    # Verify task has additional_params
    assert task.additional_params == {
        "proxy_links": ["http://proxy1.example.com", "http://proxy2.example.com"],
        "public_links": "http://public.example.com",
        "custom_field": "custom_value"
    }

    # Verify to_dict includes additional_params at root level
    task_dict = task.to_dict()
    assert task_dict["proxy_links"] == ["http://proxy1.example.com", "http://proxy2.example.com"]
    assert task_dict["public_links"] == "http://public.example.com"
    assert task_dict["custom_field"] == "custom_value"

    # Verify standard fields are still present
    assert task_dict["task_id"] == task.task_id
    assert task_dict["task_name"] == "test_task_with_params"
    assert task_dict["status"] == "queued"


def test_task_without_additional_params(modelq_instance):
    """Test that tasks work normally without additional_params."""

    @modelq_instance.task()
    def test_task_no_params(x):
        return x * 2

    # Call task without additional_params
    task = test_task_no_params(5)

    # Verify task has empty additional_params
    assert task.additional_params == {}

    # Verify to_dict doesn't include extra fields
    task_dict = task.to_dict()
    assert "proxy_links" not in task_dict
    assert "public_links" not in task_dict

    # Verify standard fields are still present
    assert task_dict["task_id"] == task.task_id
    assert task_dict["task_name"] == "test_task_no_params"
    assert task_dict["status"] == "queued"


def test_task_additional_params_in_redis(modelq_instance):
    """Test that additional_params are stored correctly in Redis."""

    @modelq_instance.task()
    def redis_task(value):
        return value

    # Create task with additional_params
    task = redis_task(
        "test",
        additional_params={
            "metadata": {"key": "value"},
            "tags": ["tag1", "tag2"]
        }
    )

    # Retrieve task from Redis
    task_json = modelq_instance.redis_client.get(f"task:{task.task_id}")
    assert task_json is not None

    stored_task = _json_bytes_to_dict(task_json)

    # Verify additional_params are stored
    assert stored_task["metadata"] == {"key": "value"}
    assert stored_task["tags"] == ["tag1", "tag2"]


def test_task_from_dict_with_additional_params(modelq_instance):
    """Test that Task.from_dict correctly restores additional_params."""
    from modelq.app.tasks.base import Task

    # Create a task dict with additional params
    task_dict = {
        "task_id": "test_123",
        "task_name": "test_task",
        "payload": {"data": {}},
        "status": "queued",
        "result": None,
        "created_at": time.time(),
        "queued_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "stream": False,
        "proxy_links": ["http://proxy.example.com"],
        "public_links": "http://public.example.com",
        "custom_metadata": {"foo": "bar"}
    }

    # Restore task from dict
    task = Task.from_dict(task_dict)

    # Verify additional_params are restored
    assert task.additional_params == {
        "proxy_links": ["http://proxy.example.com"],
        "public_links": "http://public.example.com",
        "custom_metadata": {"foo": "bar"}
    }

    # Verify to_dict produces same output
    restored_dict = task.to_dict()
    assert restored_dict["proxy_links"] == ["http://proxy.example.com"]
    assert restored_dict["public_links"] == "http://public.example.com"
    assert restored_dict["custom_metadata"] == {"foo": "bar"}


def test_task_additional_params_empty_dict(modelq_instance):
    """Test that passing empty additional_params works correctly."""

    @modelq_instance.task()
    def empty_params_task(x):
        return x

    # Call with empty dict
    task = empty_params_task(5, additional_params={})

    assert task.additional_params == {}
    task_dict = task.to_dict()

    # Should only have standard fields
    expected_keys = {
        "task_id", "task_name", "payload", "status", "result",
        "created_at", "queued_at", "started_at", "finished_at", "stream"
    }
    assert set(task_dict.keys()) == expected_keys


def test_task_additional_params_with_pydantic(mock_redis):
    """Test that additional_params work with Pydantic schema validation."""
    from pydantic import BaseModel

    class TaskInput(BaseModel):
        name: str
        value: int

    mq = ModelQ(redis_client=mock_redis)

    @mq.task(schema=TaskInput)
    def pydantic_task(params):
        return params.name

    # Create task with Pydantic input and additional_params
    task = pydantic_task(
        TaskInput(name="test", value=42),
        additional_params={
            "request_id": "req_123",
            "priority": "high"
        }
    )

    assert task.additional_params == {
        "request_id": "req_123",
        "priority": "high"
    }

    task_dict = task.to_dict()
    assert task_dict["request_id"] == "req_123"
    assert task_dict["priority"] == "high"


def test_get_task_details_with_additional_params(modelq_instance):
    """Test that get_task_details returns additional_params."""
    task_data = {
        "task_id": "detail_with_params",
        "task_name": "test_task",
        "status": "queued",
        "created_at": time.time(),
        "proxy_links": ["http://proxy.example.com"],
        "public_links": "http://public.example.com"
    }

    modelq_instance.redis_client.set(
        "task_history:detail_with_params",
        json.dumps(task_data)
    )
    modelq_instance.redis_client.zadd(
        "task_history",
        {"detail_with_params": task_data["created_at"]}
    )

    details = modelq_instance.get_task_details("detail_with_params")
    assert details is not None
    assert details["proxy_links"] == ["http://proxy.example.com"]
    assert details["public_links"] == "http://public.example.com"
