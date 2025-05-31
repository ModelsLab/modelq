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
