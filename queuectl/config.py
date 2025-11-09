from typing import Any, Dict, Optional
from .models import Config
from .db import Database


class ConfigManager:
    def __init__(self, database: Database):
        self.db = database
        self._defaults = {
            'max_retries': '3',
            'backoff_base': '2',
            'poll_interval_ms': '500',
            'db_path': '.data/queuectl.db',
            'worker_heartbeat_interval_seconds': '5',
            'stale_worker_timeout_seconds': '30'
        }
    
    def get(self, key: str) -> Optional[str]:
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
            row = cursor.fetchone()
            if row:
                return row[0]
            return self._defaults.get(key)
    
    def set(self, key: str, value: str):
        with self.db.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO config (key, value) 
                VALUES (?, ?)
            """, (key, value))
    
    def list_all(self) -> Dict[str, str]:
        result = self._defaults.copy()
        
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT key, value FROM config")
            for row in cursor.fetchall():
                result[row[0]] = row[1]
        
        return result
    
    def get_config(self) -> Config:
        config_dict = self.list_all()
        
        return Config(
            max_retries=int(config_dict.get('max_retries', '3')),
            backoff_base=int(config_dict.get('backoff_base', '2')),
            poll_interval_ms=int(config_dict.get('poll_interval_ms', '500')),
            db_path=config_dict.get('db_path', '.data/queuectl.db'),
            default_timeout_seconds=int(config_dict['default_timeout_seconds']) if config_dict.get('default_timeout_seconds') else None,
            log_dir=config_dict.get('log_dir'),
            max_concurrent_processes_per_worker=int(config_dict.get('max_concurrent_processes_per_worker', '1')),
            worker_heartbeat_interval_seconds=int(config_dict.get('worker_heartbeat_interval_seconds', '5')),
            stale_worker_timeout_seconds=int(config_dict.get('stale_worker_timeout_seconds', '30'))
        )