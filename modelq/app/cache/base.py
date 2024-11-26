import sqlite3
import os
import sys
import click
import json

from modelq.app.tasks import Task
from typing import Optional

class Cache :

    def __init__(self,db_path : str = "cache.db") -> None:
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
    
    def store_task(self, task: Task) -> None:
        """Stores a new task or updates an existing one in the SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT OR REPLACE INTO tasks (task_id, task_name, payload, status, result, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (task.task_id, task.task_name, json.dumps(task.payload), task.status, task.result, task.timestamp)
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
                    "result": row[4],
                    "timestamp": row[5]
                })
            return None