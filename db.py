# db.py
import sqlite3
import os
import json
import time
import threading
from typing import Optional, Dict, Any

# default path (can be overridden by init_db)
DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")
_db_lock = threading.Lock()

def _get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path: Optional[str] = None):
    """
    Initialize DB tables. Accepts optional db_path so callers can pass DB_PATH from env.
    Backwards-compatible: calling init_db() with no args keeps current behavior.
    """
    global DB_PATH
    if db_path:
        DB_PATH = db_path

    conn = _get_conn()
    cur = conn.cursor()
    # jobs table: id (text), owner_id, payload (text JSON), status, mode, created_at, expires_at
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      owner_id INTEGER NOT NULL,
      payload TEXT NOT NULL,
      status TEXT NOT NULL,
      mode TEXT,
      created_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL
    )
    """)
    # meta table for simple heartbeat or other keys
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT
    )
    """)
    conn.commit()
    conn.close()

def set_meta(key: str, value: str):
    with _db_lock:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
        conn.close()

def get_meta(key: str) -> Optional[str]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else None

# job helpers (existing names kept)
def save_job_row(job_id: str, owner_id: int, payload: Dict[str, Any], status: str, expires_at: int, mode: Optional[str]=None):
    """
    Low-level insert/replace for jobs (keeps your original API).
    """
    with _db_lock:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO jobs (id, owner_id, payload, status, mode, created_at, expires_at) VALUES (?,?,?,?,?,?,?)",
            (job_id, owner_id, json.dumps(payload), status, mode, int(time.time()), expires_at)
        )
        conn.commit()
        conn.close()

def save_job(job_id: str, payload: Any, status: str = "waiting_target", owner_id: int = 0, expires_after_sec: int = 24*3600, mode: Optional[str] = None):
    """
    Convenience wrapper used by older code that called db.save_job(...).
    - job_id: string key (e.g. "pending:123456")
    - payload: any JSON-serializable payload (string or dict)
    - owner_id: owner sending the job (0 if unknown)
    - expires_after_sec: TTL in seconds from now (default 24h)
    """
    if not isinstance(payload, (str, bytes)):
        payload_to_store = payload
    else:
        # if payload is a string, try to store as string inside JSON for consistency
        payload_to_store = {"text": payload}
    expires_at = int(time.time()) + int(expires_after_sec)
    save_job_row(job_id=job_id, owner_id=owner_id, payload=payload_to_store, status=status, expires_at=expires_at, mode=mode)

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    try:
        payload = json.loads(row["payload"])
    except Exception:
        payload = row["payload"]
    return {
        "id": row["id"],
        "owner_id": row["owner_id"],
        "payload": payload,
        "status": row["status"],
        "mode": row["mode"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"]
    }

def get_pending_job() -> Optional[Dict[str, Any]]:
    """
    Return the most recent job with status = 'waiting_target' (or None).
    Useful for owner -> reply with chat id flow.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT 1", ("waiting_target",))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    try:
        payload = json.loads(row["payload"])
    except Exception:
        payload = row["payload"]
    return {
        "id": row["id"],
        "owner_id": row["owner_id"],
        "payload": payload,
        "status": row["status"],
        "mode": row["mode"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"]
    }

def update_job_status(job_id: str, status: str, mode: Optional[str]=None):
    with _db_lock:
        conn = _get_conn()
        cur = conn.cursor()
        if mode is not None:
            cur.execute("UPDATE jobs SET status = ?, mode = ? WHERE id = ?", (status, mode, job_id))
        else:
            cur.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
        conn.commit()
        conn.close()

def delete_job(job_id: str):
    with _db_lock:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()
        conn.close()
