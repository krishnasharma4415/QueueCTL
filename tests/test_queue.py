import pytest
import tempfile
import time
import os
import json
from datetime import datetime, timedelta
from queuectl.db import Database
from queuectl.queue import QueueManager
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


@pytest.fixture
def queue_manager(temp_db):
    return QueueManager(temp_db)


def test_enqueue_job_success(queue_manager):
    job_spec = '{"command": "echo hello", "id": "test-job-1"}'
    job_id = queue_manager.validate_and_enqueue(job_spec)
    
    assert job_id == "test-job-1"
    
    job = queue_manager.get_job(job_id)
    assert job is not None
    assert job.command == "echo hello"
    assert job.state == JobState.PENDING
    assert job.attempts == 0


def test_enqueue_job_auto_id(queue_manager):
    job_spec = '{"command": "echo hello"}'
    job_id = queue_manager.validate_and_enqueue(job_spec)
    
    assert job_id is not None
    assert len(job_id) > 0
    
    job = queue_manager.get_job(job_id)
    assert job is not None
    assert job.command == "echo hello"


def test_enqueue_job_duplicate_id(queue_manager):
    job_spec = '{"command": "echo hello", "id": "duplicate-job"}'
    queue_manager.validate_and_enqueue(job_spec)
    
    with pytest.raises(ValueError, match="already exists"):
        queue_manager.validate_and_enqueue(job_spec)


def test_enqueue_job_invalid_json(queue_manager):
    with pytest.raises(ValueError, match="Invalid JSON"):
        queue_manager.validate_and_enqueue('{"invalid": json}')


def test_enqueue_job_missing_command(queue_manager):
    with pytest.raises(ValueError, match="must include 'command' field"):
        queue_manager.validate_and_enqueue('{"id": "test"}')


def test_job_failure_handling(queue_manager):
    job_spec = '{"command": "echo hello", "id": "test-job", "max_retries": 2}'
    job_id = queue_manager.validate_and_enqueue(job_spec)
    
    job = queue_manager.get_job(job_id)
    original_next_run_at = job.next_run_at
    
    import time
    time.sleep(0.001)
    
    queue_manager.handle_job_failure(job, "Test error", backoff_base=2)
    
    updated_job = queue_manager.get_job(job_id)
    assert updated_job.attempts == 1
    assert updated_job.state == JobState.PENDING
    assert updated_job.last_error == "Test error"
    assert updated_job.next_run_at > original_next_run_at


def test_job_moves_to_dlq_after_max_retries(queue_manager):
    job_spec = '{"command": "echo hello", "id": "test-job", "max_retries": 1}'
    job_id = queue_manager.validate_and_enqueue(job_spec)
    
    job = queue_manager.get_job(job_id)
    
    queue_manager.handle_job_failure(job, "First error")
    queue_manager.handle_job_failure(job, "Second error")
    
    assert queue_manager.get_job(job_id) is None
    
    dlq_jobs = queue_manager.list_dlq()
    assert len(dlq_jobs) == 1
    assert dlq_jobs[0].original_job_id == job_id
    assert dlq_jobs[0].command == "echo hello"


def test_dlq_retry(queue_manager):
    job_spec = '{"command": "echo hello", "id": "test-job", "max_retries": 0}'
    job_id = queue_manager.validate_and_enqueue(job_spec)
    
    job = queue_manager.get_job(job_id)
    queue_manager.handle_job_failure(job, "Test error")
    
    dlq_jobs = queue_manager.list_dlq()
    assert len(dlq_jobs) == 1
    
    new_job_id = queue_manager.retry_from_dlq(dlq_jobs[0].id)
    
    new_job = queue_manager.get_job(new_job_id)
    assert new_job is not None
    assert new_job.command == "echo hello"
    assert new_job.state == JobState.PENDING
    assert new_job.attempts == 0
    
    dlq_jobs_after = queue_manager.list_dlq()
    assert len(dlq_jobs_after) == 0


def test_exponential_backoff_calculation(queue_manager):
    assert queue_manager.calculate_backoff_delay(0, 2) == 1
    assert queue_manager.calculate_backoff_delay(1, 2) == 2
    assert queue_manager.calculate_backoff_delay(2, 2) == 4
    assert queue_manager.calculate_backoff_delay(3, 2) == 8
    
    assert queue_manager.calculate_backoff_delay(1, 3) == 3
    assert queue_manager.calculate_backoff_delay(2, 3) == 9


def test_job_success_handling(queue_manager):
    job_spec = '{"command": "echo hello", "id": "test-job"}'
    job_id = queue_manager.validate_and_enqueue(job_spec)
    
    job = queue_manager.get_job(job_id)
    queue_manager.handle_job_success(job)
    
    updated_job = queue_manager.get_job(job_id)
    assert updated_job.state == JobState.COMPLETED
    assert updated_job.worker_id is None


def test_list_jobs_filtering(queue_manager):
    queue_manager.validate_and_enqueue('{"command": "echo 1", "id": "job1"}')
    queue_manager.validate_and_enqueue('{"command": "echo 2", "id": "job2"}')
    
    job1 = queue_manager.get_job("job1")
    queue_manager.handle_job_success(job1)
    
    all_jobs = queue_manager.list_jobs()
    assert len(all_jobs) == 2
    
    pending_jobs = queue_manager.list_jobs(state="pending")
    assert len(pending_jobs) == 1
    assert pending_jobs[0].id == "job2"
    
    completed_jobs = queue_manager.list_jobs(state="completed")
    assert len(completed_jobs) == 1
    assert completed_jobs[0].id == "job1"