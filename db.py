# db.py
import sqlite3
import os
import time
import logging
from typing import Optional

logger = logging.getLogger("quizbot.db")

DEFAULT_DB = "./quizbot.db"

def _get_conn(db_path: Optional[str] = None):
    if db_path is None:
        db_path = DEFAULT_DB
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path: Optional[str] = None):
    if db_path is None:
        db_path = DEFAULT_DB
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        payload TEXT,
        status TEXT,
        created_at INTEGER
    )
    """)
    conn.commit()
    conn.close()

def get_meta(key: str, db_path: Optional[str] = None) -> Optional[str]:
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else None

def set_meta(key: str, value: str, db_path: Optional[str] = None):
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()

def save_job(job_id: str, payload: str, status: str = "waiting_target", db_path: Optional[str] = None):
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO jobs (id, payload, status, created_at) VALUES (?, ?, ?, ?)",
                (job_id, payload, status, int(time.time())))
    conn.commit()
    conn.close()
