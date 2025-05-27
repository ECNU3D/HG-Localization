import pytest
import os
from pathlib import Path
from dotenv import load_dotenv

# This file is for testing the hg_localization.config module.
# The local manage_environment_variables fixture has been removed (was lines 9-22).
# Global environment management is now handled by tests/conftest.py.

# Test the new configuration system
from hg_localization.config import HGLocalizationConfig, default_config

EXPECTED_DEFAULT_DATASETS_STORE_PATH_PARENT = Path(__file__).parent.parent / "hg_localization" 

# --- Tests for HGLocalizationConfig class ---

def test_config_class_init_with_defaults():
    """Test that HGLocalizationConfig can be initialized with default values."""
    config = HGLocalizationConfig()
    
    assert config.s3_bucket_name is None
    assert config.s3_endpoint_url is None
    assert config.aws_access_key_id is None
    assert config.aws_secret_access_key is None
    assert config.s3_data_prefix == ""
    assert config.default_config_name == "default_config"
    assert config.default_revision_name == "default_revision"
    assert config.public_datasets_json_key == "public_datasets.json"
    assert config.public_datasets_zip_dir_prefix == "public_datasets_zip"
    
    # datasets_store_path should be set to default
    expected_default_path = EXPECTED_DEFAULT_DATASETS_STORE_PATH_PARENT / "datasets_store"
    assert config.datasets_store_path == expected_default_path

def test_config_class_init_with_custom_values(tmp_path):
    """Test that HGLocalizationConfig can be initialized with custom values."""
    custom_store_path = tmp_path / "custom_store"
    
    config = HGLocalizationConfig(
        s3_bucket_name="custom-bucket",
        s3_endpoint_url="http://custom.endpoint.com",
        aws_access_key_id="custom-key",
        aws_secret_access_key="custom-secret",
        s3_data_prefix="custom/prefix/",
        datasets_store_path=custom_store_path,
        default_config_name="custom_config",
        default_revision_name="custom_revision",
        public_datasets_json_key="custom_public.json",
        public_datasets_zip_dir_prefix="custom_zip_prefix"
    )
    
    assert config.s3_bucket_name == "custom-bucket"
    assert config.s3_endpoint_url == "http://custom.endpoint.com"
    assert config.aws_access_key_id == "custom-key"
    assert config.aws_secret_access_key == "custom-secret"
    assert config.s3_data_prefix == "custom/prefix"  # Should be stripped
    assert config.datasets_store_path == custom_store_path
    assert config.default_config_name == "custom_config"
    assert config.default_revision_name == "custom_revision"
    assert config.public_datasets_json_key == "custom_public.json"
    assert config.public_datasets_zip_dir_prefix == "custom_zip_prefix"

def test_config_s3_data_prefix_stripping():
    """Test that s3_data_prefix is properly stripped of leading/trailing slashes."""
    test_cases = [
        ("", ""),
        ("prefix", "prefix"),
        ("prefix/", "prefix"),
        ("/prefix", "prefix"),
        ("/prefix/", "prefix"),
        ("deep/nested/prefix/", "deep/nested/prefix"),
        ("/deep/nested/prefix/", "deep/nested/prefix"),
    ]
    
    for input_prefix, expected_prefix in test_cases:
        config = HGLocalizationConfig(s3_data_prefix=input_prefix)
        assert config.s3_data_prefix == expected_prefix

def test_config_from_env_method(monkeypatch, tmp_path):
    """Test that HGLocalizationConfig.from_env() loads from environment variables."""
    custom_path = tmp_path / "env_store"
    
    # Set environment variables
    monkeypatch.setenv("HGLOC_S3_BUCKET_NAME", "env-bucket")
    monkeypatch.setenv("HGLOC_S3_ENDPOINT_URL", "http://env.endpoint.com")
    monkeypatch.setenv("HGLOC_AWS_ACCESS_KEY_ID", "env-key")
    monkeypatch.setenv("HGLOC_AWS_SECRET_ACCESS_KEY", "env-secret")
    monkeypatch.setenv("HGLOC_S3_DATA_PREFIX", "env/prefix/")
    monkeypatch.setenv("HGLOC_DATASETS_STORE_PATH", str(custom_path))
    monkeypatch.setenv("HGLOC_DEFAULT_CONFIG_NAME", "env_config")
    monkeypatch.setenv("HGLOC_DEFAULT_REVISION_NAME", "env_revision")
    monkeypatch.setenv("HGLOC_PUBLIC_DATASETS_JSON_KEY", "env_public.json")
    monkeypatch.setenv("HGLOC_PUBLIC_DATASETS_ZIP_DIR_PREFIX", "env_zip_prefix")
    
    config = HGLocalizationConfig.from_env()
    
    assert config.s3_bucket_name == "env-bucket"
    assert config.s3_endpoint_url == "http://env.endpoint.com"
    assert config.aws_access_key_id == "env-key"
    assert config.aws_secret_access_key == "env-secret"
    assert config.s3_data_prefix == "env/prefix"  # Should be stripped
    assert config.datasets_store_path == custom_path
    assert config.default_config_name == "env_config"
    assert config.default_revision_name == "env_revision"
    assert config.public_datasets_json_key == "env_public.json"
    assert config.public_datasets_zip_dir_prefix == "env_zip_prefix"

def test_config_from_env_with_missing_env_vars():
    """Test that HGLocalizationConfig.from_env() handles missing environment variables gracefully."""
    config = HGLocalizationConfig.from_env()
    
    # Should use defaults for missing env vars
    assert config.s3_bucket_name is None
    assert config.s3_endpoint_url is None
    assert config.aws_access_key_id is None
    assert config.aws_secret_access_key is None
    assert config.s3_data_prefix == ""
    assert config.default_config_name == "default_config"
    assert config.default_revision_name == "default_revision"
    assert config.public_datasets_json_key == "public_datasets.json"
    assert config.public_datasets_zip_dir_prefix == "public_datasets_zip"

def test_config_is_s3_configured():
    """Test the is_s3_configured() method."""
    # Not configured - missing all required fields
    config1 = HGLocalizationConfig()
    assert not config1.is_s3_configured()
    
    # Not configured - missing some required fields
    config2 = HGLocalizationConfig(s3_bucket_name="bucket")
    assert not config2.is_s3_configured()
    
    config3 = HGLocalizationConfig(
        s3_bucket_name="bucket",
        aws_access_key_id="key"
    )
    assert not config3.is_s3_configured()
    
    # Properly configured
    config4 = HGLocalizationConfig(
        s3_bucket_name="bucket",
        aws_access_key_id="key",
        aws_secret_access_key="secret"
    )
    assert config4.is_s3_configured()

# --- Tests for backward compatibility ---

def test_default_config_instance_exists():
    """Test that the default_config instance is available."""
    from hg_localization.config import default_config
    assert isinstance(default_config, HGLocalizationConfig)

def test_backward_compatibility_global_variables():
    """Test that old global variables are still available for backward compatibility."""
    from hg_localization.config import (
        S3_BUCKET_NAME, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, 
        AWS_SECRET_ACCESS_KEY, S3_DATA_PREFIX,
        DATASETS_STORE_PATH,
        DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME,
        PUBLIC_DATASETS_JSON_KEY, PUBLIC_DATASETS_ZIP_DIR_PREFIX
    )
    
    # These should match the default_config values
    assert S3_BUCKET_NAME == default_config.s3_bucket_name
    assert S3_ENDPOINT_URL == default_config.s3_endpoint_url
    assert AWS_ACCESS_KEY_ID == default_config.aws_access_key_id
    assert AWS_SECRET_ACCESS_KEY == default_config.aws_secret_access_key
    assert S3_DATA_PREFIX == default_config.s3_data_prefix
    assert DATASETS_STORE_PATH == default_config.datasets_store_path
    assert DEFAULT_CONFIG_NAME == default_config.default_config_name
    assert DEFAULT_REVISION_NAME == default_config.default_revision_name
    assert PUBLIC_DATASETS_JSON_KEY == default_config.public_datasets_json_key
    assert PUBLIC_DATASETS_ZIP_DIR_PREFIX == default_config.public_datasets_zip_dir_prefix

def test_config_with_env_vars_affects_global_variables(monkeypatch):
    """Test that setting environment variables affects both default_config and global variables."""
    # Set environment variables
    monkeypatch.setenv("HGLOC_S3_BUCKET_NAME", "test-bucket-global")
    monkeypatch.setenv("HGLOC_AWS_ACCESS_KEY_ID", "test-key-global")
    
    # Reload the config module to pick up the new environment variables
    import hg_localization.config
    import importlib
    importlib.reload(hg_localization.config)
    
    # Check that both the default_config and global variables are updated
    from hg_localization.config import default_config, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID
    
    assert default_config.s3_bucket_name == "test-bucket-global"
    assert default_config.aws_access_key_id == "test-key-global"
    assert S3_BUCKET_NAME == "test-bucket-global"
    assert AWS_ACCESS_KEY_ID == "test-key-global"

def test_datasets_store_path_default_calculation():
    """Test that DATASETS_STORE_PATH defaults correctly when env var is not set."""
    # Create a fresh config without environment variable
    config = HGLocalizationConfig()
    
    # Default path should be relative to config.py's parent directory, then 'datasets_store'
    expected_default_path = EXPECTED_DEFAULT_DATASETS_STORE_PATH_PARENT / "datasets_store"
    assert config.datasets_store_path == expected_default_path

# Note: Testing load_dotenv() itself is tricky as it modifies os.environ globally.
# The general approach for testing modules that use load_dotenv is to:
# 1. Ensure a .env file with test values is present or mocked during test setup.
# 2. Or, more commonly, mock os.environ directly (as done with monkeypatch here)
#    to simulate what load_dotenv would have done, making the test independent of actual .env files.
# The manage_environment_variables fixture and monkeypatch usage already cover this by controlling
# the environment seen by the config.py module when it's (re)loaded.
