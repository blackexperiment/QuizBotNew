# db.py
import os
import sqlite3
import threading
import time
from typing import Optional, Any, Dict

DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")
_LOCK = threading.Lock()

def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            payload TEXT,
            status TEXT,
            created_at INTEGER
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        )
        """)
        conn.commit()
        conn.close()

def save_job(job_id: str, payload: str, status: str = "queued"):
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO jobs (job_id, payload, status, created_at) VALUES (?, ?, ?, ?)",
                    (job_id, payload, status, int(time.time())))
        conn.commit()
        conn.close()

def update_job_status(job_id: str, status: str):
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET status = ? WHERE job_id = ?", (status, job_id))
        conn.commit()
        conn.close()

def set_meta(k: str, v: str):
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?, ?)", (k, v))
        conn.commit()
        conn.close()

def get_meta(k: str) -> Optional[str]:
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT v FROM meta WHERE k = ?", (k,))
        row = cur.fetchone()
        conn.close()
        return row["v"] if row else None

def fetch_form_via_api(form_id: str) -> Dict[str, Any]:
    """
    Placeholder. If you want to fetch private Google Forms with OAuth, implement here.
    For now raise NotImplementedError so callers know.
    """
    raise NotImplementedError("Google Forms API fetching not implemented. Use manual/formatted input.")

# initialize DB on import
init_db()
