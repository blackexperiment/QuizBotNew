# db.py
import os
from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, DateTime, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bot.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(Integer, unique=True, index=True, nullable=False)
    username = Column(String, nullable=True)
    role = Column(String, default="teacher")  # 'owner' or 'teacher'
    created_at = Column(DateTime, default=datetime.utcnow)

class Chat(Base):
    __tablename__ = "chats"
    id = Column(Integer, primary_key=True, index=True)
    chat_id = Column(Integer, unique=True, nullable=False)
    name = Column(String, nullable=False)
    owner_telegram_id = Column(Integer, nullable=True)  # who added (owner/teacher)
    is_global = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class Job(Base):
    __tablename__ = "bulk_jobs"
    id = Column(Integer, primary_key=True, index=True)
    created_by = Column(Integer, nullable=False)  # telegram id
    raw_text = Column(Text, nullable=False)
    status = Column(String, default="pending")
    total_actions = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class Action(Base):
    __tablename__ = "actions"
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("bulk_jobs.id", ondelete="CASCADE"))
    seq = Column(Integer, nullable=False)
    type = Column(String, nullable=False)  # MSG or POLL
    payload = Column(JSON, nullable=False)
    status = Column(String, default="pending")

class JobTarget(Base):
    __tablename__ = "job_targets"
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("bulk_jobs.id", ondelete="CASCADE"))
    chat_id = Column(Integer, nullable=False)
    status = Column(String, default="pending")

def init_db():
    Base.metadata.create_all(bind=engine)

def get_session():
    return SessionLocal()
