import pytest
import tempfile
import time
import os
from queuectl.db import Database
from queuectl.config import ConfigManager


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
def config_manager(temp_db):
    return ConfigManager(temp_db)


def test_get_default_config(config_manager):
    assert config_manager.get('max_retries') == '3'
    assert config_manager.get('backoff_base') == '2'
    assert config_manager.get('poll_interval_ms') == '500'


def test_set_and_get_config(config_manager):
    config_manager.set('max_retries', '5')
    assert config_manager.get('max_retries') == '5'
    
    config_manager.set('custom_key', 'custom_value')
    assert config_manager.get('custom_key') == 'custom_value'


def test_get_nonexistent_key(config_manager):
    assert config_manager.get('nonexistent_key') is None


def test_list_all_config(config_manager):
    config_manager.set('test_key', 'test_value')
    
    all_config = config_manager.list_all()
    
    assert 'max_retries' in all_config
    assert 'test_key' in all_config
    assert all_config['test_key'] == 'test_value'


def test_get_config_object(config_manager):
    config_manager.set('max_retries', '5')
    config_manager.set('backoff_base', '3')
    
    config = config_manager.get_config()
    
    assert config.max_retries == 5
    assert config.backoff_base == 3
    assert config.poll_interval_ms == 500
    assert config.db_path == '.data/queuectl.db'


def test_config_persistence(temp_db):
    config_manager1 = ConfigManager(temp_db)
    config_manager1.set('test_persistence', 'persistent_value')
    
    config_manager2 = ConfigManager(temp_db)
    assert config_manager2.get('test_persistence') == 'persistent_value'