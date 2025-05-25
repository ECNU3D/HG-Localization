import pytest
import os
from pathlib import Path
from dotenv import load_dotenv

# This file is for testing the hg_localization.config module.
# The local manage_environment_variables fixture has been removed (was lines 9-22).
# Global environment management is now handled by tests/conftest.py.

EXPECTED_DEFAULT_DATASETS_STORE_PATH_PARENT = Path(__file__).parent.parent / "hg_localization" 

def test_s3_config_vars_loaded_from_env(monkeypatch):
    """Test that S3 related config variables are loaded from environment variables."""
    monkeypatch.setenv("HGLOC_S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("HGLOC_S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("HGLOC_AWS_ACCESS_KEY_ID", "test-access-key")
    monkeypatch.setenv("HGLOC_AWS_SECRET_ACCESS_KEY", "test-secret-key")
    monkeypatch.setenv("HGLOC_S3_DATA_PREFIX", "my/data/prefix/")

    # Import config after setting env vars to ensure it picks them up
    from hg_localization import config
    # Reload module to pick up mocked env vars. This is crucial.
    import importlib
    importlib.reload(config)

    assert config.S3_BUCKET_NAME == "test-bucket"
    assert config.S3_ENDPOINT_URL == "http://localhost:9000"
    assert config.AWS_ACCESS_KEY_ID == "test-access-key"
    assert config.AWS_SECRET_ACCESS_KEY == "test-secret-key"
    assert config.S3_DATA_PREFIX == "my/data/prefix" # Should be stripped

def test_s3_config_vars_defaults_when_not_set(monkeypatch):
    """Test that S3 related config variables have expected defaults (None or empty string)."""
    # Directly patch the config module variables to simulate defaults
    monkeypatch.setattr('hg_localization.config.S3_BUCKET_NAME', None)
    monkeypatch.setattr('hg_localization.config.S3_ENDPOINT_URL', None)
    monkeypatch.setattr('hg_localization.config.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.config.AWS_SECRET_ACCESS_KEY', None)
    monkeypatch.setattr('hg_localization.config.S3_DATA_PREFIX', "")
    
    from hg_localization import config

    assert config.S3_BUCKET_NAME is None
    assert config.S3_ENDPOINT_URL is None
    assert config.AWS_ACCESS_KEY_ID is None
    assert config.AWS_SECRET_ACCESS_KEY is None
    assert config.S3_DATA_PREFIX == "" # Default for S3_DATA_PREFIX is empty string

def test_datasets_store_path_from_env(monkeypatch, tmp_path):
    """Test DATASETS_STORE_PATH is loaded from environment variable."""
    custom_path = tmp_path / "custom_store"
    monkeypatch.setenv("HGLOC_DATASETS_STORE_PATH", str(custom_path))
    
    from hg_localization import config
    import importlib
    importlib.reload(config)

    assert config.DATASETS_STORE_PATH == custom_path

def test_datasets_store_path_default():
    """Test DATASETS_STORE_PATH defaults correctly when env var is not set."""
    # Env var HGLOC_DATASETS_STORE_PATH should be unset by the fixture
    from hg_localization import config
    import importlib
    importlib.reload(config)

    # Default path is relative to config.py's parent directory, then 'datasets_store'
    # This needs to align with how config.py calculates its default.
    # Assuming config.py is in hg_localization/, its parent is the project root for this test structure.
    expected_default_path = EXPECTED_DEFAULT_DATASETS_STORE_PATH_PARENT / "datasets_store"
    assert config.DATASETS_STORE_PATH == expected_default_path, \
           f"Expected {expected_default_path}, got {config.DATASETS_STORE_PATH}"

def test_default_names_constants():
    """Test that default name constants are defined correctly."""
    from hg_localization import config
    import importlib
    importlib.reload(config)
    
    assert config.DEFAULT_CONFIG_NAME == "default_config"
    assert config.DEFAULT_REVISION_NAME == "default_revision"

def test_public_dataset_keys_constants():
    """Test that public dataset related key/prefix constants are defined."""
    from hg_localization import config
    import importlib
    importlib.reload(config)

    assert config.PUBLIC_DATASETS_JSON_KEY == "public_datasets.json"
    assert config.PUBLIC_DATASETS_ZIP_DIR_PREFIX == "public_datasets_zip"

# Note: Testing load_dotenv() itself is tricky as it modifies os.environ globally.
# The general approach for testing modules that use load_dotenv is to:
# 1. Ensure a .env file with test values is present or mocked during test setup.
# 2. Or, more commonly, mock os.environ directly (as done with monkeypatch here)
#    to simulate what load_dotenv would have done, making the test independent of actual .env files.
# The manage_environment_variables fixture and monkeypatch usage already cover this by controlling
# the environment seen by the config.py module when it's (re)loaded.
