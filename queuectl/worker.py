import signal
import subprocess
import time
import uuid
import socket
import os
import sys
from datetime import datetime
from typing import Optional
from .models import Job, Worker
from .db import Database
from .queue import QueueManager
from .config import ConfigManager
from .logging_utils import setup_logging
from .version import __version__


class WorkerProcess:
    def __init__(self, database: Database, poll_interval_ms: int = 500):
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.db = database
        self.queue_manager = QueueManager(database)
        self.config_manager = ConfigManager(database)
        self.poll_interval_ms = poll_interval_ms
        self.stopping = False
        self.current_job: Optional[Job] = None
        self.logger = setup_logging()
        
        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, self._signal_handler)
        
        if sys.platform == "win32" and hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, self._signal_handler)
        
        self._register_worker()
    
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Worker {self.worker_id} received signal {signum}, shutting down gracefully")
        self.stopping = True
    
    def _register_worker(self):
        worker = Worker(
            worker_id=self.worker_id,
            pid=os.getpid(),
            started_at=datetime.utcnow(),
            last_heartbeat_at=datetime.utcnow(),
            hostname=socket.gethostname(),
            version=__version__
        )
        self.db.register_worker(worker)
        self.logger.info(f"Worker {self.worker_id} registered")
    
    def _update_heartbeat(self):
        self.db.update_worker_heartbeat(self.worker_id)
    
    def run(self):
        self.logger.info(f"Worker {self.worker_id} starting main loop")
        last_heartbeat = datetime.utcnow()
        
        try:
            while not self.stopping:
                last_heartbeat = self._update_heartbeat_if_needed(last_heartbeat)
                
                job = self.claim_job()
                if job:
                    self.current_job = job
                    self.execute_job(job)
                    self.current_job = None
                else:
                    time.sleep(self.poll_interval_ms / 1000)
        finally:
            self.cleanup()
    
    def _update_heartbeat_if_needed(self, last_heartbeat: datetime) -> datetime:
        now = datetime.utcnow()
        if (now - last_heartbeat).total_seconds() >= 5:
            self._update_heartbeat()
            return now
        return last_heartbeat
    
    def claim_job(self) -> Optional[Job]:
        return self.db.claim_job(self.worker_id)
    
    def execute_job(self, job: Job):
        self.logger.info(f"Worker {self.worker_id} executing job {job.id}: {job.command}")
        start_time = datetime.utcnow()
        
        try:
            config = self.config_manager.get_config()
            timeout = job.timeout_seconds or config.default_timeout_seconds
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            if result.returncode == 0:
                self._handle_success(job, duration)
            else:
                self._handle_command_failure(job, result, duration, config.backoff_base)
                
        except subprocess.TimeoutExpired:
            self._handle_timeout(job, start_time, timeout)
            
        except Exception as e:
            self._handle_execution_error(job, start_time, e)
    
    def _handle_success(self, job: Job, duration: float):
        self.logger.info(f"Worker {self.worker_id} completed job {job.id} successfully in {duration:.2f}s")
        self.queue_manager.handle_job_success(job)
    
    def _handle_command_failure(self, job: Job, result, duration: float, backoff_base: int):
        error_msg = f"Command failed with exit code {result.returncode}"
        if result.stderr:
            error_msg += f": {result.stderr.strip()[:500]}"
        
        self.logger.warning(f"Worker {self.worker_id} job {job.id} failed in {duration:.2f}s: {error_msg}")
        self.queue_manager.handle_job_failure(job, error_msg, backoff_base)
    
    def _handle_timeout(self, job: Job, start_time: datetime, timeout: int):
        duration = (datetime.utcnow() - start_time).total_seconds()
        error_msg = f"Command timed out after {timeout} seconds"
        self.logger.warning(f"Worker {self.worker_id} job {job.id} timed out after {duration:.2f}s")
        config = self.config_manager.get_config()
        self.queue_manager.handle_job_failure(job, error_msg, config.backoff_base)
    
    def _handle_execution_error(self, job: Job, start_time: datetime, error: Exception):
        duration = (datetime.utcnow() - start_time).total_seconds()
        error_msg = f"Execution error: {str(error)}"
        self.logger.error(f"Worker {self.worker_id} job {job.id} failed with error after {duration:.2f}s: {error_msg}")
        config = self.config_manager.get_config()
        self.queue_manager.handle_job_failure(job, error_msg, config.backoff_base)
    
    def cleanup(self):
        self.logger.info(f"Worker {self.worker_id} cleaning up")
        
        if self.current_job:
            self.logger.warning(f"Worker {self.worker_id} interrupted while processing job {self.current_job.id}")
            config = self.config_manager.get_config()
            self.queue_manager.handle_job_failure(
                self.current_job, 
                "Worker interrupted during execution",
                config.backoff_base
            )
        
        self.db.unregister_worker(self.worker_id)
        self.logger.info(f"Worker {self.worker_id} shutdown complete")