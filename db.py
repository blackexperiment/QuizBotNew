# db.py
import sqlite3
import time
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger("quizbot.db")
_DB_PATH = None

def init_db(path: str = "./quizbot.db"):
    global _DB_PATH
    _DB_PATH = path
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    # jobs table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      payload TEXT,
      status TEXT,
      owner_id INTEGER,
      extra TEXT,
      created_at INTEGER
    )
    """)
    # meta table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT
    )
    """)
    conn.commit()
    conn.close()

def _get_conn():
    global _DB_PATH
    if _DB_PATH is None:
        raise RuntimeError("DB not initialized")
    conn = sqlite3.connect(_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def save_job(job_id: str, payload: str, status: str = "new", owner_id: Optional[int] = None):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO jobs (id, payload, status, owner_id, extra, created_at) VALUES (?,?,?,?,?,?)",
                (job_id, payload, status, owner_id, None, int(time.time())))
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
    return dict(row)

def update_job_status(job_id: str, status: str, extra: Optional[str] = None):
    conn = _get_conn()
    cur = conn.cursor()
    if extra is not None:
        cur.execute("UPDATE jobs SET status = ?, extra = ? WHERE id = ?", (status, extra, job_id))
    else:
        cur.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
    conn.commit()
    conn.close()

def get_latest_job_for_owner(owner_id: int, status: Optional[str] = None):
    conn = _get_conn()
    cur = conn.cursor()
    if status:
        cur.execute("SELECT * FROM jobs WHERE owner_id = ? AND status = ? ORDER BY created_at DESC LIMIT 1", (owner_id, status))
    else:
        cur.execute("SELECT * FROM jobs WHERE owner_id = ? ORDER BY created_at DESC LIMIT 1", (owner_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return dict(row)
    return None

def set_meta(key: str, value: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()

def get_meta(key: str) -> Optional[str]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row["value"]
    return None
