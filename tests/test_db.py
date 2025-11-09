import pytest
import tempfile
import time
import os
from datetime import datetime, timedelta
from queuectl.db import Database
from queuectl.models import Job, Worker, JobState


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db = Database(db_path)
    yield db
    
    try:
        import gc
        gc.collect()
        time.sleep(0.1)
        os.unlink(db_path)
    except (OSError, PermissionError):
        time.sleep(0.5)
        try:
            os.unlink(db_path)
        except (OSError, PermissionError):
            pass


def test_database_initialization(temp_db):
    with temp_db.connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        assert 'jobs' in tables
        assert 'dlq' in tables
        assert 'config' in tables
        assert 'workers' in tables


def test_worker_registration(temp_db):
    worker = Worker(
        worker_id="test-worker-1",
        pid=12345,
        started_at=datetime.utcnow(),
        last_heartbeat_at=datetime.utcnow(),
        hostname="localhost",
        version="0.1.0"
    )
    
    temp_db.register_worker(worker)
    
    workers = temp_db.get_active_workers()
    assert len(workers) == 1
    assert workers[0].worker_id == "test-worker-1"


def test_worker_heartbeat_update(temp_db):
    worker = Worker(
        worker_id="test-worker-1",
        pid=12345,
        started_at=datetime.utcnow(),
        last_heartbeat_at=datetime.utcnow() - timedelta(minutes=1),
        hostname="localhost",
        version="0.1.0"
    )
    
    temp_db.register_worker(worker)
    temp_db.update_worker_heartbeat("test-worker-1")
    
    workers = temp_db.get_active_workers()
    assert len(workers) == 1
    assert workers[0].last_heartbeat_at > worker.last_heartbeat_at


def test_worker_unregistration(temp_db):
    worker = Worker(
        worker_id="test-worker-1",
        pid=12345,
        started_at=datetime.utcnow(),
        last_heartbeat_at=datetime.utcnow(),
        hostname="localhost",
        version="0.1.0"
    )
    
    temp_db.register_worker(worker)
    temp_db.unregister_worker("test-worker-1")
    
    workers = temp_db.get_active_workers()
    assert len(workers) == 0


def test_claim_job_no_jobs(temp_db):
    job = temp_db.claim_job("test-worker-1")
    assert job is None


def test_claim_job_success(temp_db):
    now = datetime.utcnow()
    
    with temp_db.transaction() as conn:
        conn.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, 
                            created_at, updated_at, next_run_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "test-job-1", "echo hello", "pending", 0, 3,
            now.isoformat(), now.isoformat(), now.isoformat()
        ))
    
    job = temp_db.claim_job("test-worker-1")
    
    assert job is not None
    assert job.id == "test-job-1"
    assert job.state == JobState.PROCESSING
    assert job.worker_id == "test-worker-1"


def test_claim_job_concurrent_access(temp_db):
    now = datetime.utcnow()
    
    with temp_db.transaction() as conn:
        conn.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, 
                            created_at, updated_at, next_run_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "test-job-1", "echo hello", "pending", 0, 3,
            now.isoformat(), now.isoformat(), now.isoformat()
        ))
    
    job1 = temp_db.claim_job("worker-1")
    job2 = temp_db.claim_job("worker-2")
    
    assert job1 is not None
    assert job2 is None
    assert job1.worker_id == "worker-1"


def test_claim_job_respects_next_run_at(temp_db):
    now = datetime.utcnow()
    future = now + timedelta(hours=1)
    
    with temp_db.transaction() as conn:
        conn.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, 
                            created_at, updated_at, next_run_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "test-job-1", "echo hello", "pending", 0, 3,
            now.isoformat(), now.isoformat(), future.isoformat()
        ))
    
    job = temp_db.claim_job("test-worker-1")
    assert job is None