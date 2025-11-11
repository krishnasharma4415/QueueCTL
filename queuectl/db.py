import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from .models import Job, Worker, JobState


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_db_directory()
        self._init_schema()
    
    def _ensure_db_directory(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def _init_schema(self):
        with self.connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'completed', 'failed')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    next_run_at TEXT NOT NULL,
                    last_error TEXT,
                    priority INTEGER DEFAULT 0,
                    run_at TEXT,
                    timeout_seconds INTEGER,
                    worker_id TEXT,
                    FOREIGN KEY (worker_id) REFERENCES workers(worker_id)
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_state_next_run ON jobs(state, next_run_at);
                CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
                CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC, created_at ASC);

                CREATE TABLE IF NOT EXISTS dlq (
                    id TEXT PRIMARY KEY,
                    original_job_id TEXT NOT NULL,
                    command TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    moved_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_dlq_moved_at ON dlq(moved_at);

                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    last_heartbeat_at TEXT NOT NULL,
                    hostname TEXT NOT NULL,
                    version TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers(last_heartbeat_at);
            """)
    
    @contextmanager
    def connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            yield conn
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def transaction(self):
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    
    def claim_job(self, worker_id: str) -> Optional[Job]:
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id FROM jobs 
                WHERE state = 'pending' 
                AND datetime(next_run_at) <= datetime('now')
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """)
            
            job_row = cursor.fetchone()
            if not job_row:
                return None
                
            job_id = job_row[0]
            
            cursor.execute("""
                UPDATE jobs 
                SET state = 'processing', 
                    worker_id = ?, 
                    updated_at = datetime('now')
                WHERE id = ? 
                AND state = 'pending' 
                AND datetime(next_run_at) <= datetime('now')
            """, (worker_id, job_id))
            
            if cursor.rowcount == 1:
                return self._get_job_by_id(conn, job_id)
            else:
                return None
    
    def _get_job_by_id(self, conn, job_id: str) -> Optional[Job]:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, command, state, attempts, max_retries, created_at, 
                   updated_at, next_run_at, last_error, priority, run_at, 
                   timeout_seconds, worker_id
            FROM jobs WHERE id = ?
        """, (job_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
            
        return Job(
            id=row[0],
            command=row[1],
            state=JobState(row[2]),
            attempts=row[3],
            max_retries=row[4],
            created_at=datetime.fromisoformat(row[5]),
            updated_at=datetime.fromisoformat(row[6]),
            next_run_at=datetime.fromisoformat(row[7]),
            last_error=row[8],
            priority=row[9] or 0,
            run_at=datetime.fromisoformat(row[10]) if row[10] else None,
            timeout_seconds=row[11],
            worker_id=row[12]
        )
    
    def register_worker(self, worker: Worker):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO workers 
                (worker_id, pid, started_at, last_heartbeat_at, hostname, version)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                worker.worker_id, worker.pid, worker.started_at.isoformat(),
                worker.last_heartbeat_at.isoformat(), worker.hostname, worker.version
            ))
    
    def update_worker_heartbeat(self, worker_id: str):
        with self.transaction() as conn:
            conn.execute("""
                UPDATE workers 
                SET last_heartbeat_at = datetime('now')
                WHERE worker_id = ?
            """, (worker_id,))
    
    def unregister_worker(self, worker_id: str):
        with self.transaction() as conn:
            conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))
    
    def get_active_workers(self, timeout_seconds: int = 30) -> List[Worker]:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT worker_id, pid, started_at, last_heartbeat_at, hostname, version
                FROM workers 
                WHERE datetime(last_heartbeat_at, '+{} seconds') > datetime('now')
            """.format(timeout_seconds))
            
            workers = []
            for row in cursor.fetchall():
                workers.append(Worker(
                    worker_id=row[0],
                    pid=row[1],
                    started_at=datetime.fromisoformat(row[2]),
                    last_heartbeat_at=datetime.fromisoformat(row[3]),
                    hostname=row[4],
                    version=row[5]
                ))
            return workers
    
    def recover_stale_jobs(self, stale_timeout_seconds: int = 30, backoff_base: int = 2):
        stale_jobs = self._find_stale_jobs(stale_timeout_seconds)
        
        from .queue import QueueManager
        queue_manager = QueueManager(self)
        
        for job in stale_jobs:
            error_msg = f"Job recovered from stale worker {job.worker_id}"
            queue_manager.handle_job_failure(job, error_msg, backoff_base)
        
        return len(stale_jobs)
    
    def _find_stale_jobs(self, stale_timeout_seconds: int) -> List[Job]:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT j.id, j.command, j.state, j.attempts, j.max_retries, 
                       j.created_at, j.updated_at, j.next_run_at, j.last_error, 
                       j.priority, j.run_at, j.timeout_seconds, j.worker_id
                FROM jobs j
                LEFT JOIN workers w ON j.worker_id = w.worker_id
                WHERE j.state = 'processing' 
                AND (w.worker_id IS NULL 
                     OR datetime(w.last_heartbeat_at, '+{} seconds') < datetime('now'))
            """.format(stale_timeout_seconds))
            
            stale_jobs = []
            for row in cursor.fetchall():
                job = Job(
                    id=row[0],
                    command=row[1],
                    state=JobState(row[2]),
                    attempts=row[3],
                    max_retries=row[4],
                    created_at=datetime.fromisoformat(row[5]),
                    updated_at=datetime.fromisoformat(row[6]),
                    next_run_at=datetime.fromisoformat(row[7]),
                    last_error=row[8],
                    priority=row[9] or 0,
                    run_at=datetime.fromisoformat(row[10]) if row[10] else None,
                    timeout_seconds=row[11],
                    worker_id=row[12]
                )
                stale_jobs.append(job)
            return stale_jobs