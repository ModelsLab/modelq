import sqlite3
import os
import json
from typing import Optional
from modelq.app.tasks import Task

class Cache:

    def __init__(self, db_path: str = "cache.db") -> None:
        self.db_path = db_path
        self._initialize_db()
    
    def _initialize_db(self):
        """Initializes the SQLite database if it doesn't exist."""
        if not os.path.exists(self.db_path):
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''
                    CREATE TABLE tasks (
                        task_id TEXT PRIMARY KEY,
                        task_name TEXT,
                        payload TEXT,
                        status TEXT,
                        result TEXT,
                        timestamp REAL
                    )
                    '''
                )
                conn.commit()
    
    def _convert_to_string(self, data) -> str:
        """Converts any data type to a string representation."""
        try:
            if isinstance(data, (dict, list, int, float, bool)):
                return json.dumps(data)
            return str(data)
        except TypeError:
            return str(data)

    def store_task(self, task: Task) -> None:
        """Stores a new task or updates an existing one in the SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            task_id = self._convert_to_string(task.task_id)
            task_name = self._convert_to_string(task.task_name)
            payload = self._convert_to_string(task.payload)
            status = self._convert_to_string(task.status)
            result = self._convert_to_string(task.result)
            timestamp = task.timestamp if isinstance(task.timestamp, (int, float)) else None

            cursor.execute(
                '''
                INSERT OR REPLACE INTO tasks (task_id, task_name, payload, status, result, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (task_id, task_name, payload, status, result, timestamp)
            )
            conn.commit()

    def get_task(self, task_id: str) -> Optional[Task]:
        """Retrieves a task from the SQLite database by its task ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM tasks WHERE task_id = ?', (task_id,))
            row = cursor.fetchone()
            if row:
                return Task.from_dict({
                    "task_id": row[0],
                    "task_name": row[1],
                    "payload": json.loads(row[2]),
                    "status": row[3],
                    "result": json.loads(row[4]) if row[4] and row[4].startswith(('{', '[')) else row[4],
                    "timestamp": row[5]
                })
            return None
