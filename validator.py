# db.py
import sqlite3
import time
import json
from typing import Optional, Any

DEFAULT_DB = "./quizbot.db"

_conn = None

def _get_conn(path: Optional[str]=None):
    global _conn
    if _conn:
        return _conn
    db_path = path or DEFAULT_DB
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # create tables if not exists
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        owner_id INTEGER,
        payload TEXT,
        status TEXT,
        created_at INTEGER
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    conn.commit()
    _conn = conn
    return _conn

def init_db(path: Optional[str]=None):
    _get_conn(path)

def save_job(job_id: str, owner_id: int, payload: dict, status: str="waiting"):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO jobs (id, owner_id, payload, status, created_at) VALUES (?, ?, ?, ?, ?)",
                (job_id, owner_id, json.dumps(payload, ensure_ascii=False), status, int(time.time())))
    conn.commit()

def get_job(job_id: str) -> Optional[dict]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    r = cur.fetchone()
    if not r:
        return None
    return {"id": r["id"], "owner_id": r["owner_id"], "payload": json.loads(r["payload"]), "status": r["status"], "created_at": r["created_at"]}

def pop_next_waiting():
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE status IN ('waiting','queued') ORDER BY created_at ASC LIMIT 1")
    r = cur.fetchone()
    if not r:
        return None
    job = {"id": r["id"], "owner_id": r["owner_id"], "payload": json.loads(r["payload"]), "status": r["status"], "created_at": r["created_at"]}
    cur.execute("DELETE FROM jobs WHERE id = ?", (r["id"],))
    conn.commit()
    return job

def set_meta(key: str, value: Any):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, json.dumps(value, ensure_ascii=False)))
    conn.commit()

def get_meta(key: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
    r = cur.fetchone()
    if not r:
        return None
    return json.loads(r["value"])
