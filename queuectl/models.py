from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class JobState(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    id: str
    command: str
    state: JobState
    attempts: int
    max_retries: int
    created_at: datetime
    updated_at: datetime
    next_run_at: datetime
    last_error: Optional[str] = None
    priority: int = 0
    run_at: Optional[datetime] = None
    timeout_seconds: Optional[int] = None
    worker_id: Optional[str] = None


@dataclass
class Worker:
    worker_id: str
    pid: int
    started_at: datetime
    last_heartbeat_at: datetime
    hostname: str
    version: str


@dataclass
class Config:
    max_retries: int = 3
    backoff_base: int = 2
    poll_interval_ms: int = 500
    db_path: str = ".data/queuectl.db"
    default_timeout_seconds: Optional[int] = None
    log_dir: Optional[str] = None
    max_concurrent_processes_per_worker: int = 1
    worker_heartbeat_interval_seconds: int = 5
    stale_worker_timeout_seconds: int = 30


@dataclass
class DLQJob:
    id: str
    original_job_id: str
    command: str
    attempts: int
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime
    moved_at: datetime