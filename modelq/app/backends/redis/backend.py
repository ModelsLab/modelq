import time
import json
import redis
from typing import Optional
from modelq.app.backends.base import QueueBackend


class RedisQueueBackend(QueueBackend):
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    # ─────────────────────── Task Queue ───────────────────────────────

    def enqueue_task(self, task_data: dict) -> None:
        task_data["status"] = "queued"
        self.redis.rpush("ml_tasks", json.dumps(task_data))
        self.redis.zadd("queued_requests", {task_data["task_id"]: task_data["queued_at"]})

    def dequeue_task(self, timeout: Optional[int] = None) -> Optional[dict]:
        data = self.redis.blpop("ml_tasks", timeout=timeout or 5)
        if data:
            _, task_json = data
            return json.loads(task_json)
        return None

    def requeue_task(self, task_data: dict) -> None:
        self.redis.rpush("ml_tasks", json.dumps(task_data))

    def enqueue_delayed_task(self, task_data: dict, delay_seconds: int) -> None:
        run_at = time.time() + delay_seconds
        self.redis.zadd("delayed_tasks", {json.dumps(task_data): run_at})

    def dequeue_ready_delayed_tasks(self) -> list:
        now = time.time()
        tasks = self.redis.zrangebyscore("delayed_tasks", 0, now)
        for task_json in tasks:
            self.redis.zrem("delayed_tasks", task_json)
            self.redis.lpush("ml_tasks", task_json)
        return [json.loads(t) for t in tasks]

    def flush_queue(self) -> None:
        self.redis.ltrim("ml_tasks", 1, 0)

    # ─────────────────────── Task State ───────────────────────────────

    def save_task_state(self, task_id: str, task_data: dict, result: bool) -> None:
        task_data["finished_at"] = time.time()
        self.redis.set(f"task_result:{task_id}", json.dumps(task_data), ex=3600)
        self.redis.set(f"task:{task_id}", json.dumps(task_data), ex=86400)

    def load_task_state(self, task_id: str) -> Optional[dict]:
        data = self.redis.get(f"task:{task_id}")
        return json.loads(data) if data else None

    def remove_task_from_queue(self, task_id: str) -> bool:
        tasks = self.redis.lrange("ml_tasks", 0, -1)
        for task_json in tasks:
            task_dict = json.loads(task_json)
            if task_dict.get("task_id") == task_id:
                self.redis.lrem("ml_tasks", 1, task_json)
                self.redis.zrem("queued_requests", task_id)
                return True
        return False

    def mark_processing(self, task_id: str) -> bool:
        return self.redis.sadd("processing_tasks", task_id) == 1

    def unmark_processing(self, task_id: str) -> None:
        self.redis.srem("processing_tasks", task_id)

    def get_all_processing_tasks(self) -> list:
        return [pid.decode() for pid in self.redis.smembers("processing_tasks")]

    def get_all_queued_tasks(self) -> list:
        raw = self.redis.lrange("ml_tasks", 0, -1)
        return [json.loads(task) for task in raw if json.loads(task).get("status") == "queued"]

    # ─────────────────────── Server State ───────────────────────────────

    def register_server(self, server_id: str, task_names: list) -> None:
        self.redis.hset("servers", server_id, json.dumps({
            "allowed_tasks": task_names,
            "status": "idle",
            "last_heartbeat": time.time()
        }))

    def update_server_status(self, server_id: str, status: str) -> None:
        raw = self.redis.hget("servers", server_id)
        if raw:
            data = json.loads(raw)
            data["status"] = status
            data["last_heartbeat"] = time.time()
            self.redis.hset("servers", server_id, json.dumps(data))

    def get_all_server_ids(self) -> list:
        return [k.decode("utf-8") for k in self.redis.hkeys("servers")]

    def get_server_data(self, server_id: str) -> Optional[dict]:
        raw = self.redis.hget("servers", server_id)
        return json.loads(raw) if raw else None

    def prune_dead_servers(self, timeout: int) -> list:
        now = time.time()
        pruned = []
        for sid, raw in self.redis.hgetall("servers").items():
            try:
                sid_str = sid.decode()
                data = json.loads(raw)
                if now - data.get("last_heartbeat", 0) > timeout:
                    self.redis.hdel("servers", sid_str)
                    pruned.append(sid_str)
            except:
                continue
        return pruned

    # ─────────────────────── Miscellaneous ─────────────────────────────

    def prune_old_results(self, older_than_seconds: int) -> int:
        now = time.time()
        deleted = 0
        for key in self.redis.scan_iter("task_result:*"):
            raw = self.redis.get(key)
            if not raw:
                continue
            data = json.loads(raw)
            timestamp = data.get("finished_at") or data.get("started_at")
            if timestamp and now - timestamp > older_than_seconds:
                task_id = key.decode().split(":")[-1]
                self.redis.delete(key)
                self.redis.delete(f"task:{task_id}")
                deleted += 1
        return deleted

    def queue_length(self) -> int:
        return self.redis.llen("ml_tasks")

    def cleanup_dlq(self) -> None:
        self.redis.delete("dlq")
