import time
import json
import redis
from typing import Optional
from modelq.app.backends.base import QueueBackend


class RedisQueueBackend(QueueBackend):

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self._register_scripts()

    # ─────────────────────── Task Queue ───────────────────────────────
    def _register_scripts(self) -> None:
        # enqueue_task  (list + sorted-set in one shot)
        self._enqueue_sha = self.redis.script_load("""
            -- KEYS[1] = ml_tasks, KEYS[2] = queued_requests
            -- ARGV[1] = full task JSON, ARGV[2] = task_id, ARGV[3] = queued_at
            redis.call('RPUSH', KEYS[1], ARGV[1])
            redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
            return 1
        """)

        # dequeue_ready_delayed_tasks (atomically move due jobs)
        self._promote_delayed_sha = self.redis.script_load("""
            -- KEYS[1] = delayed_tasks, KEYS[2] = ml_tasks, ARGV[1] = now
            local ready = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
            if #ready == 0 then return {} end
            redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
            for i=1,#ready do
                redis.call('LPUSH', KEYS[2], ready[i])
            end
            return ready   -- array of JSON strings
        """)

        # remove_task_from_queue  (search & delete server-side)
        self._remove_sha = self.redis.script_load("""
            -- KEYS[1] = ml_tasks, KEYS[2] = queued_requests, ARGV[1] = task_id
            local len   = redis.call('LLEN', KEYS[1])
            for i=0, len-1 do
                local item = redis.call('LINDEX', KEYS[1], i)
                if item then
                    local ok, obj = pcall(cjson.decode, item)
                    if ok and obj['task_id'] == ARGV[1] then
                        redis.call('LSET',  KEYS[1], i, '__DEL__')
                        redis.call('LREM',  KEYS[1], 0,  '__DEL__')
                        redis.call('ZREM',  KEYS[2], ARGV[1])
                        return 1
                    end
                end
            end
            return 0
        """)

    def enqueue_task(self, task_data: dict) -> None:
        task_data["status"] = "queued"
        self.redis.evalsha(
            self._enqueue_sha,
            2,                       # number of KEYS
            "ml_tasks", "queued_requests",
            json.dumps(task_data),   # ARGV[1]
            task_data["task_id"],    # ARGV[2]
            task_data["queued_at"],  # ARGV[3]
        )
            
    def dequeue_task(self, timeout: Optional[int] = None) -> Optional[dict]:
        rv = self.redis.blpop("ml_tasks", timeout or 5)
        if rv:
            _, raw = rv
            return json.loads(raw)
        return None
    
    def requeue_task(self, task_data: dict) -> None:
        self.redis.rpush("ml_tasks", json.dumps(task_data))

    def enqueue_delayed_task(self, task_data: dict, delay_seconds: int) -> None:
        run_at = time.time() + delay_seconds
        self.redis.zadd("delayed_tasks", {json.dumps(task_data): run_at})

    def dequeue_ready_delayed_tasks(self) -> list:
        # Single RTT instead of ZRANGEBYSCORE + loop in Python
        ready = self.redis.evalsha(
            self._promote_delayed_sha,
            2, "delayed_tasks", "ml_tasks", time.time()
        )
        return [json.loads(j) for j in ready]

    def flush_queue(self) -> None:
        self.redis.ltrim("ml_tasks", 1, 0)

    # ─────────────────────── Task State ───────────────────────────────

    def save_task_state(self, task_id: str, task_data: dict, result: bool) -> None:
        task_data["finished_at"] = time.time()
        with self.redis.pipeline() as pipe:       # tiny but measurable
            pipe.set(f"task_result:{task_id}", json.dumps(task_data), ex=3600)
            pipe.set(f"task:{task_id}",         json.dumps(task_data), ex=86400)
            pipe.execute()

    def load_task_state(self, task_id: str) -> Optional[dict]:
        data = self.redis.get(f"task:{task_id}")
        return json.loads(data) if data else None

    def remove_task_from_queue(self, task_id: str) -> bool:
        return bool(self.redis.evalsha(
            self._remove_sha,
            2, "ml_tasks", "queued_requests", task_id
        ))

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
