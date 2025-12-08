# db.py - minimal sqlite wrapper for jobs and meta
import sqlite3
import os
import time
from typing import Optional, Any, Dict

DB_PATH = os.environ.get("DB_PATH", "./quizbot.db")

# Use row factory
def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        payload TEXT,
        status TEXT,
        owner_id INTEGER,
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
    conn.close()

def save_job(job_id: str, payload: str, owner_id: int, status: str="waiting_target"):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO jobs (id, payload, status, owner_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (job_id, payload, status, owner_id, int(time.time())))
    conn.commit()
    conn.close()

def fetch_most_recent_waiting(owner_id: Optional[int]=None):
    conn = _get_conn()
    cur = conn.cursor()
    if owner_id:
        cur.execute("SELECT id, payload FROM jobs WHERE status = ? AND owner_id = ? ORDER BY created_at DESC LIMIT 1", ("waiting_target", owner_id))
    else:
        cur.execute("SELECT id, payload FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT 1", ("waiting_target",))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def mark_job_status(job_id: str, status: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
    conn.commit()
    conn.close()

def get_job(job_id: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def delete_job(job_id: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
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
