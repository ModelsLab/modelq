import pytest
import fakeredis
import time
import json
from modelq import ModelQ
from modelq.app.tasks import Task

@pytest.fixture
def mock_redis():
    return fakeredis.FakeStrictRedis()

@pytest.fixture
def modelq_instance(mock_redis):
    return ModelQ(redis_client=mock_redis)

def test_register_server(modelq_instance):
    modelq_instance.register_server()
    server_data = modelq_instance.redis_client.hget("servers", modelq_instance.server_id)
    assert server_data is not None

def test_modelq_initialization(modelq_instance):
    assert modelq_instance.server_id is not None
    assert modelq_instance.redis_client is not None

def test_enqueue_task(modelq_instance):
    task_data = {"task_id": "123", "status": "new"}
    payload = {"data": "sample"}
    modelq_instance.enqueue_task(task_data, payload)
    queued_task = modelq_instance.redis_client.lpop("ml_tasks")
    assert queued_task is not None
    assert b'"task_id": "123"' in queued_task

def test_requeue_stuck_processing_tasks(modelq_instance):
    task_id = "stuck_task"
    task_data = {
        "task_id": task_id,
        "status": "processing",
        "started_at": time.time() - 200
    }
    modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_data))
    modelq_instance.redis_client.sadd("processing_tasks", task_id)
    modelq_instance.requeue_stuck_processing_tasks(threshold=180)
    queued_task = modelq_instance.redis_client.lpop("ml_tasks")
    assert queued_task is not None
    queued_task_data = json.loads(queued_task)
    assert queued_task_data["task_id"] == task_id
    assert queued_task_data["status"] == "queued"

def test_prune_old_task_results(modelq_instance):
    old_task_id = "old_task"
    old_task_data = {
        "task_id": old_task_id,
        "status": "completed",
        "finished_at": time.time() - 90000
    }
    modelq_instance.redis_client.set(f"task_result:{old_task_id}", json.dumps(old_task_data))
    modelq_instance.prune_old_task_results(older_than_seconds=86400)
    pruned_task = modelq_instance.redis_client.get(f"task_result:{old_task_id}")
    assert pruned_task is None

def test_heartbeat(modelq_instance):
    modelq_instance.register_server()
    initial_data = json.loads(modelq_instance.redis_client.hget("servers", modelq_instance.server_id))
    initial_heartbeat = initial_data["last_heartbeat"]
    time.sleep(1)
    modelq_instance.heartbeat()
    updated_data = json.loads(modelq_instance.redis_client.hget("servers", modelq_instance.server_id))
    updated_heartbeat = updated_data["last_heartbeat"]
    assert updated_heartbeat > initial_heartbeat

def mock_task_function():
    return "Task Completed"

def test_enqueue_and_retrieve_task(modelq_instance):
    task_data = {
        "task_id": "task_456",
        "status": "new"
    }
    payload = {"data": "sample"}
    modelq_instance.enqueue_task(task_data, payload)
    queued_task = modelq_instance.redis_client.lpop("ml_tasks")
    assert queued_task is not None
    queued_task_data = json.loads(queued_task)
    assert queued_task_data["task_id"] == "task_456"
    assert queued_task_data["status"] == "queued"

def test_enqueue_delayed_task(modelq_instance):
    task_data = {
        "task_id": "delayed_task",
        "status": "new"
    }
    delay_seconds = 10
    modelq_instance.enqueue_delayed_task(task_data, delay_seconds)
    delayed_tasks = modelq_instance.redis_client.zrangebyscore("delayed_tasks", 0, time.time() + delay_seconds)
    assert len(delayed_tasks) == 1
    delayed_task_data = json.loads(delayed_tasks[0])
    assert delayed_task_data["task_id"] == "delayed_task"

def test_get_all_queued_tasks(modelq_instance):
    modelq_instance.redis_client.delete("ml_tasks")
    tasks = [
        {"task_id": "task_1", "status": "queued"},
        {"task_id": "task_2", "status": "queued"}
    ]
    for task in tasks:
        modelq_instance.redis_client.rpush("ml_tasks", json.dumps(task))
    queued_tasks = modelq_instance.get_all_queued_tasks()
    assert len(queued_tasks) == 2
    assert queued_tasks[0]["task_id"] == "task_1"
    assert queued_tasks[1]["task_id"] == "task_2"

def test_get_registered_server_ids(modelq_instance):
    modelq_instance.register_server()
    server_ids = modelq_instance.get_registered_server_ids()
    assert modelq_instance.server_id in server_ids
