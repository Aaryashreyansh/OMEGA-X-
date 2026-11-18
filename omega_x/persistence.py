from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import asdict
from pathlib import Path

from .dag import Task
from .engine import TaskResult
from .storage import ContentAddressedStorage


class ExecutionJournal:
    """Durable task-state journal; payload values live in CAS, not SQLite.

    Successful outputs must be JSON-serializable. A RUNNING task found after
    restart is reset to PENDING because its worker may have been interrupted.
    """
    TERMINAL = {"SUCCESS", "FAILED", "BLOCKED"}

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.storage = ContentAddressedStorage(self.root / "cas")
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.root / "journal.sqlite", check_same_thread=False)
        with self._lock:
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS task_state (
                    task_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    output_digest TEXT,
                    error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    duration REAL NOT NULL DEFAULT 0
                )
            """)
            self._db.commit()

    @staticmethod
    def fingerprint(task: Task) -> str:
        encoded = json.dumps(asdict(task), sort_keys=True, separators=(",", ":"),
                             allow_nan=False).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def restore(self, tasks: dict[str, Task]) -> dict[str, TaskResult]:
        restored: dict[str, TaskResult] = {}
        with self._lock:
            rows = self._db.execute(
                "SELECT task_id,fingerprint,status,output_digest,error,attempts,duration FROM task_state"
            ).fetchall()
            for task_id, fingerprint, status, digest, error, attempts, duration in rows:
                if task_id not in tasks:
                    continue
                if fingerprint != self.fingerprint(tasks[task_id]):
                    raise ValueError(
                        f"Task definition changed for {task_id!r}; use a new state directory "
                        "or clear the old journal deliberately"
                    )
                if status == "RUNNING":
                    self._db.execute(
                        "UPDATE task_state SET status='PENDING',output_digest=NULL,error=NULL WHERE task_id=?",
                        (task_id,),
                    )
                    continue
                if status == "SUCCESS":
                    if not digest:
                        raise RuntimeError(f"Missing output digest for successful task {task_id}")
                    value = self.storage.get_json(digest)
                    restored[task_id] = TaskResult(task_id, status, value=value,
                                                   attempts=attempts, duration=duration)
                elif status in {"FAILED", "BLOCKED"}:
                    restored[task_id] = TaskResult(task_id, status, error=error,
                                                   attempts=attempts, duration=duration)
            self._db.commit()
        return restored

    def mark_running(self, task: Task) -> None:
        fingerprint = self.fingerprint(task)
        with self._lock:
            self._db.execute("""
                INSERT INTO task_state(task_id,fingerprint,status)
                VALUES(?,?,'RUNNING')
                ON CONFLICT(task_id) DO UPDATE SET
                    fingerprint=excluded.fingerprint,status='RUNNING',
                    output_digest=NULL,error=NULL
            """, (task.id, fingerprint))
            self._db.commit()

    def save_result(self, task: Task, result: TaskResult) -> None:
        digest = self.storage.put_json(result.value) if result.status == "SUCCESS" else None
        fingerprint = self.fingerprint(task)
        with self._lock:
            self._db.execute("""
                INSERT INTO task_state(task_id,fingerprint,status,output_digest,error,attempts,duration)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    fingerprint=excluded.fingerprint,status=excluded.status,
                    output_digest=excluded.output_digest,error=excluded.error,
                    attempts=excluded.attempts,duration=excluded.duration
            """, (task.id, fingerprint, result.status, digest, result.error,
                  result.attempts, result.duration))
            self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()
        self.storage.close()
