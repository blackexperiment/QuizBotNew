import sqlite3
import threading
import os
from typing import Optional, List, Dict, Any

DB_PATH = os.environ.get("DB_PATH", "quizbot.db")
_lock = threading.Lock()

def _conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with _lock:
        conn = _conn()
        cur = conn.cursor()
        # jobs table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS quiz_jobs (
            job_id TEXT PRIMARY KEY,
            title TEXT,
            owner_id INTEGER,
            target_chat INTEGER,
            total_questions INTEGER,
            status TEXT,
            created_at INTEGER,
            updated_at INTEGER
        )
        """)
        # posts table: one row per question
        cur.execute("""
        CREATE TABLE IF NOT EXISTS quiz_posts (
            job_id TEXT,
            q_index INTEGER,
            question TEXT,
            options TEXT,
            correct_letter TEXT,
            attempts INTEGER DEFAULT 0,
            status TEXT,
            message_id INTEGER,
            last_error TEXT,
            last_error_at INTEGER,
            PRIMARY KEY (job_id, q_index)
        )
        """)
        conn.commit()
        conn.close()

def save_job(job_id: str, title: str, owner_id: int, target_chat: int, total: int):
    import time
    with _lock:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""
        INSERT OR REPLACE INTO quiz_jobs (job_id, title, owner_id, target_chat, total_questions, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (job_id, title, owner_id, target_chat, total, "pending", int(time.time()), int(time.time())))
        conn.commit()
        conn.close()

def save_post(job_id: str, q_index: int, question: str, options: str, correct_letter: str):
    with _lock:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""
        INSERT OR REPLACE INTO quiz_posts (job_id, q_index, question, options, correct_letter, attempts, status)
        VALUES (?, ?, ?, ?, ?, 0, 'pending')
        """, (job_id, q_index, question, options, correct_letter))
        conn.commit()
        conn.close()

def mark_post_posted(job_id: str, q_index: int, message_id: int):
    import time
    with _lock:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""
        UPDATE quiz_posts SET status='posted', message_id=?, last_error=NULL WHERE job_id=? AND q_index=?
        """, (message_id, job_id, q_index))
        cur.execute("UPDATE quiz_jobs SET status='running', updated_at=? WHERE job_id=?", (int(time.time()), job_id))
        conn.commit()
        conn.close()

def increment_attempt(job_id: str, q_index: int, last_error: Optional[str]=None):
    import time
    with _lock:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("UPDATE quiz_posts SET attempts = attempts + 1, last_error=?, last_error_at=? WHERE job_id=? AND q_index=?",
                    (last_error, int(time.time()), job_id, q_index))
        conn.commit()
        conn.close()

def mark_post_failed(job_id: str, q_index: int, last_error: str):
    import time
    with _lock:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""
        UPDATE quiz_posts SET status='failed', last_error=?, last_error_at=? WHERE job_id=? AND q_index=?
        """, (last_error, int(time.time()), job_id, q_index))
        conn.commit()
        conn.close()

def get_next_pending_post(job_id: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM quiz_posts WHERE job_id=? AND status='pending' ORDER BY q_index ASC LIMIT 1
    """, (job_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def get_job(job_id: str) -> Optional[Dict[str,Any]]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM quiz_jobs WHERE job_id=?", (job_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None

def set_job_status(job_id: str, status: str):
    import time
    with _lock:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("UPDATE quiz_jobs SET status=?, updated_at=? WHERE job_id=?", (status, int(time.time()), job_id))
        conn.commit()
        conn.close()

def get_progress(job_id: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as done FROM quiz_posts WHERE job_id=? AND status='posted'", (job_id,))
    done = cur.fetchone()["done"]
    cur.execute("SELECT total_questions FROM quiz_jobs WHERE job_id=?", (job_id,))
    total_row = cur.fetchone()
    total = total_row["total_questions"] if total_row else 0
    conn.close()
    return done, total

def list_pending_jobs():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM quiz_jobs WHERE status IN ('pending','running') ORDER BY created_at ASC")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]
