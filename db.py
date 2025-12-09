# db.py
import sqlite3
import os
import json
import time
from typing import Optional, Dict, Any

DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")

def _get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = _get_conn()
    cur = conn.cursor()
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
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT
    )
    """)
    conn.commit()
    conn.close()

def set_meta(key: str, value: str):
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

def save_job_row(job_id: str, owner_id: int, payload: Dict[str, Any], status: str, expires_at: int, mode: Optional[str]=None):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO jobs (id, owner_id, payload, status, mode, created_at, expires_at) VALUES (?,?,?,?,?,?,?)",
                (job_id, owner_id, json.dumps(payload), status, mode, int(time.time()), expires_at))
    conn.commit()
    conn.close()

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row["id"],
        "owner_id": row["owner_id"],
        "payload": json.loads(row["payload"]),
        "status": row["status"],
        "mode": row["mode"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"]
    }

def find_pending_job_for_owner(owner_id: int) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE owner_id = ? AND status = ? ORDER BY created_at DESC LIMIT 1", (owner_id, "waiting_target"))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": row["id"],
        "owner_id": row["owner_id"],
        "payload": json.loads(row["payload"]),
        "status": row["status"],
        "mode": row["mode"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"]
    }

def update_job_status(job_id: str, status: str, mode: Optional[str]=None):
    conn = _get_conn()
    cur = conn.cursor()
    if mode is not None:
        cur.execute("UPDATE jobs SET status = ?, mode = ? WHERE id = ?", (status, mode, job_id))
    else:
        cur.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
    conn.commit()
    conn.close()

def delete_job(job_id: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()
