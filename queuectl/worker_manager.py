import multiprocessing
import signal
import subprocess
import sys
import time
import os
from pathlib import Path
from typing import List
from .worker import WorkerProcess
from .db import Database
from .logging_utils import setup_logging


class WorkerManager:
    def __init__(self, db_path: str = '.data/queuectl.db'):
        self.db_path = db_path
        self.processes: List[multiprocessing.Process] = []
        self.logger = setup_logging()
        self.pid_file = Path('.data/queuectl_workers.pid')
        
    def start_workers(self, count: int, poll_interval_ms: int = 500, detach: bool = False):
        self.logger.info(f"Starting {count} worker processes")
        
        for i in range(count):
            process = multiprocessing.Process(
                target=self._worker_main,
                args=(self.db_path, poll_interval_ms),
                name=f"queuectl-worker-{i+1}"
            )
            process.start()
            self.processes.append(process)
            self.logger.info(f"Started worker process {process.pid}")
        
        self._save_pids()
        
        if not detach:
            try:
                for process in self.processes:
                    process.join()
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, stopping workers")
                self.stop_workers()
    
    def _worker_main(self, db_path: str, poll_interval_ms: int):
        database = Database(db_path)
        worker = WorkerProcess(database, poll_interval_ms)
        worker.run()
    
    def _save_pids(self):
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.pid_file, 'w') as f:
            for process in self.processes:
                f.write(f"{process.pid}\n")
    
    def _load_pids(self) -> List[int]:
        if not self.pid_file.exists():
            return []
        
        pids = []
        with open(self.pid_file, 'r') as f:
            for line in f:
                try:
                    pids.append(int(line.strip()))
                except ValueError:
                    continue
        return pids
    
    def stop_workers(self):
        pids = self._load_pids()
        
        if not pids:
            self.logger.info("No worker PIDs found")
            return
        
        self.logger.info(f"Stopping {len(pids)} worker processes")
        
        for pid in pids:
            try:
                if sys.platform == "win32":
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except OSError:
                        os.kill(pid, signal.CTRL_C_EVENT)
                else:
                    os.kill(pid, signal.SIGTERM)
                self.logger.info(f"Sent termination signal to worker process {pid}")
            except ProcessLookupError:
                self.logger.warning(f"Worker process {pid} not found")
            except (PermissionError, OSError) as e:
                self.logger.error(f"Error stopping worker process {pid}: {e}")
        
        time.sleep(2)
        
        for pid in pids:
            try:
                os.kill(pid, 0)
                self.logger.warning(f"Worker process {pid} still running, sending force kill")
                if sys.platform == "win32":
                    try:
                        subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                                     capture_output=True, check=False)
                    except FileNotFoundError:
                        os.kill(pid, signal.SIGTERM)
                else:
                    os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        
        if self.pid_file.exists():
            self.pid_file.unlink()
        
        self.logger.info("All workers stopped")