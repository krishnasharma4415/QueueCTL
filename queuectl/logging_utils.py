import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional


def setup_logging(log_dir: Optional[str] = None, worker_id: Optional[str] = None, level: str = 'INFO'):
    logger = logging.getLogger('queuectl')
    
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, level.upper()))
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_dir and worker_id:
        log_path = Path(log_dir) / f'worker_{worker_id}.log'
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_path, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def setup_job_logging(log_dir: str, job_id: str) -> Optional[logging.FileHandler]:
    if not log_dir:
        return None
    
    job_log_path = Path(log_dir) / 'jobs' / f'{job_id}.log'
    job_log_path.parent.mkdir(parents=True, exist_ok=True)
    
    job_handler = logging.FileHandler(job_log_path)
    job_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    job_handler.setFormatter(job_formatter)
    
    return job_handler