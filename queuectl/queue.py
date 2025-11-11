import json
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from .models import Job, DLQJob, JobState
from .db import Database


class QueueManager:
    def __init__(self, database: Database):
        self.db = database
    
    def enqueue_job(self, job: Job) -> str:
        with self.db.transaction() as conn:
            self._insert_job(conn, job)
        return job.id
    
    def _insert_job(self, conn, job: Job):
        conn.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, 
                            created_at, updated_at, next_run_at, priority, 
                            run_at, timeout_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.id, job.command, job.state.value, job.attempts, job.max_retries,
            job.created_at.isoformat(), job.updated_at.isoformat(), 
            job.next_run_at.isoformat(), job.priority,
            job.run_at.isoformat() if job.run_at else None,
            job.timeout_seconds
        ))
    
    def create_job_from_spec(self, job_spec: Dict[str, Any], default_max_retries: int = 3) -> Job:
        job_id = job_spec.get('id', str(uuid.uuid4()))
        command = job_spec.get('command')
        
        if not command:
            raise ValueError("Job specification must include 'command' field")
        
        if not isinstance(command, str):
            raise ValueError("Command must be a string")
        
        now = datetime.utcnow()
        
        return Job(
            id=job_id,
            command=command,
            state=JobState.PENDING,
            attempts=0,
            max_retries=job_spec.get('max_retries', default_max_retries),
            created_at=now,
            updated_at=now,
            next_run_at=datetime.fromisoformat(job_spec['run_at']) if job_spec.get('run_at') else now,
            priority=job_spec.get('priority', 0),
            run_at=datetime.fromisoformat(job_spec['run_at']) if job_spec.get('run_at') else None,
            timeout_seconds=job_spec.get('timeout_seconds')
        )
    
    def validate_and_enqueue(self, job_spec_str: str, default_max_retries: int = 3) -> str:
        job_spec = self._parse_job_spec(job_spec_str)
        self._validate_job_spec(job_spec)
        job = self.create_job_from_spec(job_spec, default_max_retries)
        return self.enqueue_job(job)
    
    def _parse_job_spec(self, job_spec_str: str) -> Dict[str, Any]:
        try:
            job_spec = json.loads(job_spec_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")
        
        if not isinstance(job_spec, dict):
            raise ValueError("Job specification must be a JSON object")
        
        return job_spec
    
    def _validate_job_spec(self, job_spec: Dict[str, Any]):
        if 'id' in job_spec:
            existing_job = self.get_job(job_spec['id'])
            if existing_job:
                raise ValueError(f"Job with ID '{job_spec['id']}' already exists")
    
    def get_job(self, job_id: str) -> Optional[Job]:
        with self.db.connection() as conn:
            return self.db._get_job_by_id(conn, job_id)
    
    def update_job(self, job: Job):
        with self.db.transaction() as conn:
            conn.execute("""
                UPDATE jobs 
                SET command = ?, state = ?, attempts = ?, max_retries = ?,
                    updated_at = ?, next_run_at = ?, last_error = ?,
                    priority = ?, run_at = ?, timeout_seconds = ?, worker_id = ?
                WHERE id = ?
            """, (
                job.command, job.state.value, job.attempts, job.max_retries,
                job.updated_at.isoformat(), job.next_run_at.isoformat(),
                job.last_error, job.priority,
                job.run_at.isoformat() if job.run_at else None,
                job.timeout_seconds, job.worker_id, job.id
            ))
    
    def calculate_backoff_delay(self, attempts: int, base: int = 2) -> int:
        return base ** attempts
    
    def handle_job_failure(self, job: Job, error_message: str, backoff_base: int = 2):
        job.attempts += 1
        job.last_error = (error_message[:1000] if error_message else None)
        job.updated_at = datetime.utcnow()
        job.worker_id = None
        
        if job.attempts > job.max_retries:
            self.move_to_dlq(job)
            return
        
        delay_seconds = self.calculate_backoff_delay(job.attempts, backoff_base)
        job.next_run_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
        job.state = JobState.PENDING
        self.update_job(job)
    
    def handle_job_success(self, job: Job):
        job.state = JobState.COMPLETED
        job.updated_at = datetime.utcnow()
        job.worker_id = None
        self.update_job(job)
    
    def list_jobs(self, state: Optional[str] = None, limit: int = 10, 
                  since: Optional[str] = None, sort: str = 'created_at') -> List[Job]:
        query, params = self._build_list_query(state, since, sort, limit)
        
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [self._row_to_job(row) for row in cursor.fetchall()]
    
    def _build_list_query(self, state: Optional[str], since: Optional[str], 
                          sort: str, limit: int) -> tuple:
        query = """
            SELECT id, command, state, attempts, max_retries, created_at, 
                   updated_at, next_run_at, last_error, priority, run_at, 
                   timeout_seconds, worker_id
            FROM jobs
        """
        params = []
        conditions = []
        
        if state:
            conditions.append("state = ?")
            params.append(state)
        
        if since:
            conditions.append("created_at >= ?")
            params.append(since)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        if sort in ['created_at', 'updated_at', 'priority']:
            if sort == 'priority':
                query += f" ORDER BY {sort} DESC, created_at ASC"
            else:
                query += f" ORDER BY {sort} DESC"
        
        query += " LIMIT ?"
        params.append(limit)
        
        return query, params
    
    def _row_to_job(self, row) -> Job:
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
    
    def get_job_counts(self) -> Dict[str, int]:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT state, COUNT(*) 
                FROM jobs 
                GROUP BY state
            """)
            
            counts = {state.value: 0 for state in JobState}
            for row in cursor.fetchall():
                counts[row[0]] = row[1]
            
            cursor.execute("SELECT COUNT(*) FROM dlq")
            counts['dlq'] = cursor.fetchone()[0]
            
            return counts
    
    def get_recent_failures(self, limit: int = 5) -> List[Job]:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, command, state, attempts, max_retries, created_at, 
                       updated_at, next_run_at, last_error, priority, run_at, 
                       timeout_seconds, worker_id
                FROM jobs
                WHERE state = 'failed' AND last_error IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT ?
            """, (limit,))
            return [self._row_to_job(row) for row in cursor.fetchall()]
    
    def move_to_dlq(self, job: Job):
        now = datetime.utcnow()
        dlq_job = DLQJob(
            id=str(uuid.uuid4()),
            original_job_id=job.id,
            command=job.command,
            attempts=job.attempts,
            last_error=job.last_error,
            created_at=job.created_at,
            updated_at=now,
            moved_at=now
        )
        
        with self.db.transaction() as conn:
            conn.execute("""
                INSERT INTO dlq (id, original_job_id, command, attempts, 
                               last_error, created_at, updated_at, moved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dlq_job.id, dlq_job.original_job_id, dlq_job.command,
                dlq_job.attempts, dlq_job.last_error,
                dlq_job.created_at.isoformat(), dlq_job.updated_at.isoformat(),
                dlq_job.moved_at.isoformat()
            ))
            
            conn.execute("DELETE FROM jobs WHERE id = ?", (job.id,))
    
    def list_dlq(self, limit: int = 10) -> List[DLQJob]:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, original_job_id, command, attempts, last_error,
                       created_at, updated_at, moved_at
                FROM dlq 
                ORDER BY moved_at DESC 
                LIMIT ?
            """, (limit,))
            
            dlq_jobs = []
            for row in cursor.fetchall():
                dlq_jobs.append(DLQJob(
                    id=row[0],
                    original_job_id=row[1],
                    command=row[2],
                    attempts=row[3],
                    last_error=row[4],
                    created_at=datetime.fromisoformat(row[5]),
                    updated_at=datetime.fromisoformat(row[6]),
                    moved_at=datetime.fromisoformat(row[7])
                ))
            return dlq_jobs
    
    def retry_from_dlq(self, job_id: str, same_id: bool = False) -> str:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT original_job_id, command, created_at
                FROM dlq WHERE id = ?
            """, (job_id,))
            
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"DLQ job with ID '{job_id}' not found")
            
            original_job_id, command, created_at_str = row
        
        new_job_id = original_job_id if same_id else str(uuid.uuid4())
        
        if same_id:
            existing_job = self.get_job(new_job_id)
            if existing_job:
                raise ValueError(f"Job with ID '{new_job_id}' already exists")
        
        now = datetime.utcnow()
        new_job = Job(
            id=new_job_id,
            command=command,
            state=JobState.PENDING,
            attempts=0,
            max_retries=3,
            created_at=datetime.fromisoformat(created_at_str),
            updated_at=now,
            next_run_at=now
        )
        
        with self.db.transaction() as conn:
            conn.execute("""
                INSERT INTO jobs (id, command, state, attempts, max_retries, 
                                created_at, updated_at, next_run_at, priority, 
                                run_at, timeout_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                new_job.id, new_job.command, new_job.state.value, 
                new_job.attempts, new_job.max_retries,
                new_job.created_at.isoformat(), new_job.updated_at.isoformat(), 
                new_job.next_run_at.isoformat(), new_job.priority,
                new_job.run_at.isoformat() if new_job.run_at else None,
                new_job.timeout_seconds
            ))
            
            conn.execute("DELETE FROM dlq WHERE id = ?", (job_id,))
        
        return new_job_id
    
    def purge_dlq(self, older_than_days: Optional[int] = None):
        with self.db.transaction() as conn:
            if older_than_days:
                cutoff_date = datetime.utcnow() - timedelta(days=older_than_days)
                conn.execute("""
                    DELETE FROM dlq WHERE moved_at < ?
                """, (cutoff_date.isoformat(),))
            else:
                conn.execute("DELETE FROM dlq")