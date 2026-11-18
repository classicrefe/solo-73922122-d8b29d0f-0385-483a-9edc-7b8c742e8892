"""SQLite 持久化层：source / task 元数据，重启后保留成功结果。"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id            TEXT PRIMARY KEY,
    filename      TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    sha256        TEXT NOT NULL,
    uploaded_at   TEXT NOT NULL,
    header_ok     INTEGER NOT NULL,
    row_delay_ms  INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL DEFAULT 'uploaded'
);
CREATE TABLE IF NOT EXISTS tasks (
    id            TEXT PRIMARY KEY,
    source_id     TEXT NOT NULL,
    stations      TEXT NOT NULL,
    temp_min      INTEGER NOT NULL,
    temp_max      INTEGER NOT NULL,
    status        TEXT NOT NULL,
    result_path   TEXT,
    matched_rows  INTEGER NOT NULL DEFAULT 0,
    error         TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
"""

# 重启时视为中断的非终态
ACTIVE_STATUSES = ("pending", "running", "cancelling")
TERMINAL_STATUSES = ("completed", "failed", "cancelled", "interrupted")


class Database:
    """每个连接带独立锁；SQLite WAL 模式支持多线程并发访问。"""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    # -- sources -----------------------------------------------------------
    def insert_source(
        self,
        source_id: str,
        filename: str,
        size_bytes: int,
        sha256: str,
        uploaded_at: str,
        row_delay_ms: int,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO sources (id, filename, size_bytes, sha256, uploaded_at,"
                " header_ok, row_delay_ms, status) VALUES (?,?,?,?,?,1,?, 'uploaded')",
                (source_id, filename, size_bytes, sha256, uploaded_at, row_delay_ms),
            )
            self._conn.commit()

    def mark_source_upload_failed(self, source_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE sources SET header_ok=0, status='rejected' WHERE id=?",
                (source_id,),
            )
            self._conn.commit()

    def set_source_status(self, source_id: str, status: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE sources SET status=? WHERE id=?", (status, source_id)
            )
            self._conn.commit()

    def get_source(self, source_id: str) -> sqlite3.Row | None:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM sources WHERE id=?", (source_id,))
            return cur.fetchone()

    def list_sources(self) -> list[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM sources ORDER BY uploaded_at")
            return cur.fetchall()

    def get_row_delay(self, source_id: str) -> int:
        row = self.get_source(source_id)
        return int(row["row_delay_ms"]) if row else 0

    # -- tasks -------------------------------------------------------------
    def insert_task(
        self,
        task_id: str,
        source_id: str,
        stations_json: str,
        temp_min: int,
        temp_max: int,
        now: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO tasks (id, source_id, stations, temp_min, temp_max,"
                " status, created_at, updated_at) VALUES (?,?,?,?,?, 'pending', ?, ?)",
                (task_id, source_id, stations_json, temp_min, temp_max, now, now),
            )
            self._conn.commit()

    def update_task(
        self,
        task_id: str,
        status: str | None = None,
        result_path: str | None = None,
        matched_rows: int | None = None,
        error: str | None = None,
        now: str | None = None,
    ) -> None:
        sets: list[str] = []
        params: list[object] = []
        if status is not None:
            sets.append("status=?")
            params.append(status)
        if result_path is not None:
            sets.append("result_path=?")
            params.append(result_path)
        if matched_rows is not None:
            sets.append("matched_rows=?")
            params.append(matched_rows)
        if error is not None:
            sets.append("error=?")
            params.append(error)
        if now is not None:
            sets.append("updated_at=?")
            params.append(now)
        if not sets:
            return
        params.append(task_id)
        with self._lock:
            self._conn.execute(
                f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", params
            )
            self._conn.commit()

    def get_task(self, task_id: str) -> sqlite3.Row | None:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
            return cur.fetchone()

    def list_tasks(self, source_id: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if source_id is None:
                cur = self._conn.execute("SELECT * FROM tasks ORDER BY created_at")
            else:
                cur = self._conn.execute(
                    "SELECT * FROM tasks WHERE source_id=? ORDER BY created_at",
                    (source_id,),
                )
            return cur.fetchall()

    def interrupt_active_tasks(self) -> list[str]:
        """重启恢复：所有非终态任务标记为 interrupted，返回任务 id 列表。"""
        with self._lock:
            placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
            cur = self._conn.execute(
                f"SELECT id FROM tasks WHERE status IN ({placeholders})",
                ACTIVE_STATUSES,
            )
            ids = [r["id"] for r in cur.fetchall()]
            self._conn.execute(
                f"UPDATE tasks SET status='interrupted',"
                f" error='服务重启，任务中断', updated_at=datetime('now')"
                f" WHERE status IN ({placeholders})",
                ACTIVE_STATUSES,
            )
            self._conn.execute(
                "UPDATE sources SET status='interrupted' WHERE status IN"
                " ('uploaded','scanning')"
            )
            self._conn.commit()
            return ids

    def close(self) -> None:
        with self._lock:
            self._conn.close()
