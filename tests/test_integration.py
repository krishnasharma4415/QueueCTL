import pytest
import tempfile
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from queuectl.db import Database
from queuectl.queue import QueueManager
from queuectl.worker import WorkerProcess
from queuectl.models import JobState


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


def test_concurrent_job_claiming(temp_db):
    queue_manager = QueueManager(temp_db)
    
    for i in range(10):
        queue_manager.validate_and_enqueue(f'{{"command": "echo job{i}", "id": "job{i}"}}')
    
    claimed_jobs = []
    
    def claim_jobs(worker_id):
        local_claims = []
        for _ in range(5):
            job = temp_db.claim_job(worker_id)
            if job:
                local_claims.append(job.id)
            time.sleep(0.01)
        return local_claims
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for i in range(3):
            future = executor.submit(claim_jobs, f"worker-{i}")
            futures.append(future)
        
        for future in futures:
            claimed_jobs.extend(future.result())
    
    assert len(claimed_jobs) == len(set(claimed_jobs))
    assert len(claimed_jobs) <= 10


def test_worker_job_execution(temp_db):
    queue_manager = QueueManager(temp_db)
    
    queue_manager.validate_and_enqueue('{"command": "echo success", "id": "success-job"}')
    queue_manager.validate_and_enqueue('{"command": "exit 1", "id": "fail-job", "max_retries": 1}')
    
    worker = WorkerProcess(temp_db, poll_interval_ms=100)
    
    def run_worker():
        start_time = time.time()
        while time.time() - start_time < 5:
            job = worker.claim_job()
            if job:
                worker.execute_job(job)
            else:
                time.sleep(0.1)
    
    worker_thread = threading.Thread(target=run_worker)
    worker_thread.start()
    worker_thread.join(timeout=10)
    
    success_job = queue_manager.get_job("success-job")
    fail_job = queue_manager.get_job("fail-job")
    
    if success_job:
        assert success_job.state == JobState.COMPLETED
    
    if fail_job:
        assert fail_job.state in [JobState.PENDING, JobState.FAILED] or fail_job is None


def test_job_recovery_after_worker_crash(temp_db):
    queue_manager = QueueManager(temp_db)
    
    queue_manager.validate_and_enqueue('{"command": "sleep 10", "id": "long-job"}')
    
    job = temp_db.claim_job("crashed-worker")
    assert job is not None
    assert job.state == JobState.PROCESSING
    
    recovered_count = temp_db.recover_stale_jobs(stale_timeout_seconds=0, backoff_base=2)
    assert recovered_count == 1
    
    recovered_job = queue_manager.get_job("long-job")
    assert recovered_job.state == JobState.PENDING
    assert recovered_job.attempts == 1
    assert "stale worker" in recovered_job.last_error


def test_database_persistence(temp_db):
    queue_manager1 = QueueManager(temp_db)
    
    queue_manager1.validate_and_enqueue('{"command": "echo persistent", "id": "persistent-job"}')
    
    job1 = queue_manager1.get_job("persistent-job")
    queue_manager1.handle_job_success(job1)
    
    queue_manager2 = QueueManager(temp_db)
    job2 = queue_manager2.get_job("persistent-job")
    
    assert job2 is not None
    assert job2.state == JobState.COMPLETED
    assert job2.command == "echo persistent"


def test_concurrent_config_access(temp_db):
    from queuectl.config import ConfigManager
    
    def update_config(key_suffix):
        config_manager = ConfigManager(temp_db)
        for i in range(10):
            config_manager.set(f"test_key_{key_suffix}", f"value_{i}")
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for i in range(3):
            future = executor.submit(update_config, i)
            futures.append(future)
        
        for future in futures:
            future.result()
    
    config_manager = ConfigManager(temp_db)
    all_config = config_manager.list_all()
    
    for i in range(3):
        key = f"test_key_{i}"
        assert key in all_config
        assert all_config[key] == "value_9"


def test_dlq_operations_under_load(temp_db):
    queue_manager = QueueManager(temp_db)
    
    for i in range(20):
        queue_manager.validate_and_enqueue(f'{{"command": "exit 1", "id": "fail-job-{i}", "max_retries": 0}}')
    
    jobs = queue_manager.list_jobs(limit=20)
    for job in jobs:
        queue_manager.handle_job_failure(job, "Simulated failure")
    
    dlq_jobs = queue_manager.list_dlq(limit=25)
    assert len(dlq_jobs) == 20
    
    for i in range(5):
        queue_manager.retry_from_dlq(dlq_jobs[i].id)
    
    remaining_dlq = queue_manager.list_dlq(limit=25)
    assert len(remaining_dlq) == 15
    
    retried_jobs = queue_manager.list_jobs(state="pending", limit=10)
    assert len(retried_jobs) == 5