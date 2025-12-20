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
