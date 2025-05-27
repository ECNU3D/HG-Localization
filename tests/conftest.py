import pyarrow as pa

# Attempt to unregister potentially conflicting Arrow extension types
EXTENSION_TYPES_TO_UNREGISTER = [
    "datasets.features.features.Array2DExtensionType",
    "datasets.features.features.Array3DExtensionType",
    "datasets.features.features.Array4DExtensionType",
    "datasets.features.features.Array5DExtensionType",
]

for ext_type_name in EXTENSION_TYPES_TO_UNREGISTER:
    try:
        pa.unregister_extension_type(ext_type_name)
        print(f"INFO: Attempted to unregister Arrow extension type: {ext_type_name}")
    except KeyError:
        print(f"INFO: Arrow extension type {ext_type_name} was not registered, no need to unregister.")
        pass
    except Exception as e:
        print(f"WARNING: An unexpected error occurred while trying to unregister {ext_type_name}: {e}")
        pass

import datasets.features

import pytest
from pathlib import Path
import tempfile
import shutil
import os
from unittest.mock import patch

@pytest.fixture
def temp_datasets_store(monkeypatch, tmp_path: Path) -> Path:
    """Create a temporary directory to act as DATASETS_STORE_PATH for tests."""
    # Create a subdirectory within tmp_path to specifically be the datasets_store
    mock_store_path = tmp_path / "test_datasets_store"
    mock_store_path.mkdir(parents=True, exist_ok=True)

    # Patch both the old global variable and the default_config for backward compatibility
    import hg_localization.config
    import hg_localization.core
    
    # Patch the global variable for backward compatibility
    monkeypatch.setattr(hg_localization.core, 'DATASETS_STORE_PATH', mock_store_path)
    monkeypatch.setattr(hg_localization.config, 'DATASETS_STORE_PATH', mock_store_path)
    
    # Also patch the default_config instance
    monkeypatch.setattr(hg_localization.config.default_config, 'datasets_store_path', mock_store_path)
    
    yield mock_store_path

@pytest.fixture
def test_config(tmp_path: Path):
    """Create a test configuration instance with temporary paths."""
    from hg_localization.config import HGLocalizationConfig
    
    test_store_path = tmp_path / "test_config_datasets_store"
    test_store_path.mkdir(parents=True, exist_ok=True)
    
    return HGLocalizationConfig(
        s3_bucket_name="test-bucket",
        s3_endpoint_url="http://localhost:9000",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        s3_data_prefix="test/data/prefix",
        datasets_store_path=test_store_path,
        default_config_name="test_config",
        default_revision_name="test_revision",
        public_datasets_json_key="test_public_datasets.json",
        public_datasets_zip_dir_prefix="test_public_datasets_zip"
    )

@pytest.fixture
def minimal_config():
    """Create a minimal configuration instance for testing."""
    from hg_localization.config import HGLocalizationConfig
    
    return HGLocalizationConfig(
        s3_bucket_name=None,
        aws_access_key_id=None,
        aws_secret_access_key=None
    )

@pytest.fixture
def mock_hf_datasets(mocker):
    """Fixture to mock Hugging Face datasets library interactions."""
    # Patch where these functions are looked up and used in the dataset_manager module
    mock_load_dataset = mocker.patch('hg_localization.dataset_manager.load_dataset')
    mock_load_from_disk = mocker.patch('hg_localization.dataset_manager.load_from_disk')

    # Configure the mock for load_dataset
    mock_dataset_instance_to_return = mocker.MagicMock(name="dataset_instance_returned_by_load_dataset")
    mock_dataset_instance_to_return.save_to_disk = mocker.MagicMock(name="save_to_disk_mock")

    # Define a default behavior for the mocked save_to_disk
    def _simulate_save_to_disk(path_arg):
        save_path = Path(path_arg)
        (save_path / "dataset_info.json").touch() 

    mock_dataset_instance_to_return.save_to_disk.side_effect = _simulate_save_to_disk
    mock_load_dataset.return_value = mock_dataset_instance_to_return

    # Configure the mock for load_from_disk
    mock_data_loaded_from_disk = mocker.MagicMock(name="data_loaded_from_disk")
    mock_data_loaded_from_disk.data = {"train": ["mock sample"], "test": ["mock_test_sample"]} 
    mock_load_from_disk.return_value = mock_data_loaded_from_disk
    
    return {
        "load_dataset": mock_load_dataset,
        "load_from_disk": mock_load_from_disk,
        "returned_dataset_instance": mock_dataset_instance_to_return,
        "data_loaded_from_disk": mock_data_loaded_from_disk
    } 

@pytest.fixture(scope='session', autouse=True)
def manage_test_environment_variables(session_mocker):
    """
    Manages environment variables for the entire test session.
    Ensures a clean environment for config loading and prevents .env interference.
    """
    # Ensure specific HGLOC_ environment variables are not set during tests
    env_vars_to_clear = [
        "HGLOC_S3_BUCKET_NAME",
        "HGLOC_S3_ENDPOINT_URL",
        "HGLOC_AWS_ACCESS_KEY_ID",
        "HGLOC_AWS_SECRET_ACCESS_KEY",
        "HGLOC_S3_DATA_PREFIX",
        "HGLOC_DATASETS_STORE_PATH",
        "HGLOC_DEFAULT_CONFIG_NAME",
        "HGLOC_DEFAULT_REVISION_NAME",
        "HGLOC_PUBLIC_DATASETS_JSON_KEY",
        "HGLOC_PUBLIC_DATASETS_ZIP_DIR_PREFIX"
    ]
    
    original_values = {var: os.environ.get(var) for var in env_vars_to_clear}

    for var in env_vars_to_clear:
        if var in os.environ:
            del os.environ[var]

    yield # Run tests

    # Restore original environment variables
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
             del os.environ[var]

# Fixture to provide mocker for session scope if needed
@pytest.fixture(scope='session')
def session_mocker(request):
    """Session-scoped mocker placeholder."""
    pass

# This fixture will be autouse and module-scoped, ensuring that before
# any test module runs, we attempt to neutralize dotenv.
@pytest.fixture(scope="module", autouse=True)
def mock_dotenv_for_module(module_mocker):
    """Patches dotenv.load_dotenv for the scope of a test module."""
    try:
        module_mocker.patch('hg_localization.config.load_dotenv', return_value=False)
    except AttributeError:
        try:
            module_mocker.patch('dotenv.load_dotenv', return_value=False)
        except AttributeError:
            pass
    yield