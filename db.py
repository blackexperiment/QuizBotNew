# db.py
import sqlite3
import threading
import time
import os

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
        # jobs: id (text), payload (text), status (text), created_at (int)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                payload TEXT,
                status TEXT,
                created_at INTEGER
            )
        """)
        # meta: key, value
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()
        conn.close()

# jobs API
def save_job(job_id: str, payload: str, status: str = "waiting_target"):
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO jobs (id, payload, status, created_at) VALUES (?, ?, ?, ?)",
                    (job_id, payload, status, int(time.time())))
        conn.commit()
        conn.close()

def get_job(job_id: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, payload, status, created_at FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def update_job_status(job_id: str, status: str):
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
        conn.commit()
        conn.close()

def get_latest_job_with_status(status: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, payload, status, created_at FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT 1", (status,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def delete_job(job_id: str):
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()
        conn.close()

# meta API
def set_meta(key: str, value: str):
    with _LOCK:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
        conn.close()

def get_meta(key: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else None
