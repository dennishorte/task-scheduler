"""SQLite persistence layer (WAL mode, single writer)."""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    project TEXT NOT NULL,
    status TEXT NOT NULL,
    phase TEXT,
    command TEXT NOT NULL,
    workdir TEXT,
    repo_ref TEXT,
    env_json TEXT,
    cores INTEGER NOT NULL DEFAULT 1,
    mem_mb INTEGER NOT NULL DEFAULT 2048,
    est_seconds REAL NOT NULL,
    timeout_seconds REAL NOT NULL,
    setup_command TEXT,
    setup_timeout_seconds REAL,
    labels_json TEXT,
    idempotency_key TEXT,
    payload_hash TEXT,
    artifact_patterns_json TEXT,
    artifacts_json TEXT,
    verdict_pattern TEXT,
    verdict TEXT,
    exit_code INTEGER,
    pgid INTEGER,
    leader_pid INTEGER,
    leader_start INTEGER,
    intended_status TEXT,
    submitted_at TEXT NOT NULL,
    started_at TEXT,
    phase_started_at TEXT,
    ended_at TEXT,
    error TEXT,
    UNIQUE(project, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_project_status ON tasks(project, status);
CREATE INDEX IF NOT EXISTS idx_tasks_submitted ON tasks(submitted_at, id);
CREATE INDEX IF NOT EXISTS idx_tasks_labels ON tasks(labels_json);

CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    ts TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id);
"""

TERMINAL = {"succeeded", "failed", "timeout", "cancelled", "lost"}


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        # Lightweight migrations for columns added after v1.
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(tasks)")}
        for col in ("repo_source", "secret_env_keys"):
            if col not in cols:
                self._conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} TEXT")
        self._conn.commit()
        self._lock = threading.Lock()

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def one(self, sql: str, params: tuple = ()) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()

    def insert_task(self, row: dict) -> None:
        cols = ", ".join(row.keys())
        marks = ", ".join("?" for _ in row)
        with self._lock:
            self._conn.execute(
                f"INSERT INTO tasks ({cols}) VALUES ({marks})", tuple(row.values())
            )
            self._conn.commit()

    def insert_tasks_atomic(self, rows: list[dict]) -> None:
        with self._lock:
            for row in rows:
                cols = ", ".join(row.keys())
                marks = ", ".join("?" for _ in row)
                self._conn.execute(
                    f"INSERT INTO tasks ({cols}) VALUES ({marks})", tuple(row.values())
                )
            self._conn.commit()

    def update_task(self, task_id: str, **cols) -> None:
        sets = ", ".join(f"{k} = ?" for k in cols)
        with self._lock:
            self._conn.execute(
                f"UPDATE tasks SET {sets} WHERE id = ?", (*cols.values(), task_id)
            )
            self._conn.commit()

    def get_task(self, task_id: str) -> dict | None:
        return self.one("SELECT * FROM tasks WHERE id = ?", (task_id,))

    def by_idempotency_key(self, project: str, key: str) -> dict | None:
        return self.one(
            "SELECT * FROM tasks WHERE project = ? AND idempotency_key = ?",
            (project, key),
        )

    def queued_tasks(self) -> list[dict]:
        return self.query(
            "SELECT * FROM tasks WHERE status = 'queued' "
            "ORDER BY est_seconds DESC, submitted_at ASC, id ASC"
        )

    def running_tasks(self) -> list[dict]:
        return self.query("SELECT * FROM tasks WHERE status = 'running'")

    def add_event(
        self, task_id: str, from_status: str | None, to_status: str, detail: str = ""
    ) -> None:
        from .core import iso  # avoid circular at module level

        self.execute(
            "INSERT INTO task_events (task_id, ts, from_status, to_status, detail) "
            "VALUES (?,?,?,?,?)",
            (task_id, iso(), from_status, to_status, detail),
        )

    def events_for(self, task_id: str) -> list[dict]:
        return self.query(
            "SELECT * FROM task_events WHERE task_id = ? ORDER BY id", (task_id,)
        )

    def delete_task(self, task_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM task_events WHERE task_id = ?", (task_id,))
            self._conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()
