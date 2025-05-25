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
    # Using tmp_path fixture provided by pytest for a unique temp directory per test function
    # For a session-scoped temp directory, you could use tempfile.TemporaryDirectory() directly
    # with a session-scoped fixture.

    # We need to mock DATASETS_STORE_PATH in the core module
    # before any of its functions are called by the tests.
    
    # Create a subdirectory within tmp_path to specifically be the datasets_store
    # This makes cleanup more explicit if we weren't using tmp_path's auto-cleanup
    mock_store_path = tmp_path / "test_datasets_store"
    mock_store_path.mkdir(parents=True, exist_ok=True)

    # Use monkeypatch to change the DATASETS_STORE_PATH in the core module
    # This ensures that the functions in core.py use this temp path
    import hg_localization.core # Import here to ensure it's the module we want to patch
    monkeypatch.setattr(hg_localization.core, 'DATASETS_STORE_PATH', mock_store_path)
    
    # print(f"Mocking DATASETS_STORE_PATH to: {mock_store_path}") # Optional: for debugging
    yield mock_store_path # This path will be used by tests

    # Cleanup is normally handled by tmp_path fixture, but if we created something outside
    # or needed more specific cleanup, it would go here.
    # print(f"Cleaning up temporary datasets store: {mock_store_path}")
    # shutil.rmtree(mock_store_path) # tmp_path handles this

@pytest.fixture
def mock_hf_datasets(mocker):
    """Fixture to mock Hugging Face datasets library interactions within core.py."""
    # Patch where these functions are looked up and used in hg_localization.core
    mock_core_load_dataset = mocker.patch('hg_localization.core.load_dataset')
    mock_core_load_from_disk = mocker.patch('hg_localization.core.load_from_disk')

    # Configure the mock for hg_localization.core.load_dataset
    # This mock should return an object that has a .save_to_disk() method,
    # and this save_to_disk method should also be a mock.
    mock_dataset_instance_to_return = mocker.MagicMock(name="dataset_instance_returned_by_load_dataset")
    mock_dataset_instance_to_return.save_to_disk = mocker.MagicMock(name="save_to_disk_mock")

    # Define a default behavior for the mocked save_to_disk (e.g., simulate creating dataset_info.json)
    def _simulate_save_to_disk(path_arg):
        save_path = Path(path_arg)
        # save_path.mkdir(parents=True, exist_ok=True) # os.makedirs is in core.py already
        (save_path / "dataset_info.json").touch() 
        # print(f"Mock save_to_disk called with: {path_arg}") # Optional: for debugging

    mock_dataset_instance_to_return.save_to_disk.side_effect = _simulate_save_to_disk
    mock_core_load_dataset.return_value = mock_dataset_instance_to_return

    # Configure the mock for hg_localization.core.load_from_disk
    # This mock should return an object that can be treated like a loaded dataset.
    mock_data_loaded_from_disk = mocker.MagicMock(name="data_loaded_from_disk")
    # Example: give it a 'data' attribute if your tests expect it.
    mock_data_loaded_from_disk.data = {"train": ["mock sample"], "test": ["mock_test_sample"]} 
    mock_core_load_from_disk.return_value = mock_data_loaded_from_disk
    
    return {
        "load_dataset": mock_core_load_dataset,                 # The mock for hg_localization.core.load_dataset
        "load_from_disk": mock_core_load_from_disk,             # The mock for hg_localization.core.load_from_disk
        "returned_dataset_instance": mock_dataset_instance_to_return, # The object returned by the mocked load_dataset
        "data_loaded_from_disk": mock_data_loaded_from_disk     # The object returned by the mocked load_from_disk
    } 

@pytest.fixture(scope='session', autouse=True)
def manage_test_environment_variables(session_mocker):
    """
    Manages environment variables for the entire test session.
    Ensures a clean environment for config loading and prevents .env interference.
    Uses session_mocker for session-scoped patching.
    """
    # Prevent dotenv from loading any .env file during tests
    # This is a more robust way than monkeypatching per module if load_dotenv is import-time.
    # We assume dotenv is an optional import or can be globally influenced.
    # If config.py directly calls os.getenv, then clearing env vars is key.
    
    # Patch dotenv.load_dotenv globally if it's consistently used.
    # This needs to be the path where load_dotenv is looked up by config.py
    # If config.py is `from dotenv import load_dotenv`, then 'dotenv.load_dotenv' is not the right target
    # it should be 'hg_localization.config.load_dotenv' if that's how it's imported and used.
    # Let's assume for now config.py might do `import dotenv; dotenv.load_dotenv()`
    # or `from dotenv import load_dotenv; load_dotenv()`.
    # A common pattern is to patch it where it's called.

    # More directly, ensure specific HGLOC_ environment variables are not set.
    env_vars_to_clear = [
        "HGLOC_S3_BUCKET_NAME",
        "HGLOC_S3_ENDPOINT_URL",
        "HGLOC_AWS_ACCESS_KEY_ID",
        "HGLOC_AWS_SECRET_ACCESS_KEY",
        "HGLOC_S3_DATA_PREFIX",
        "HGLOC_DATASETS_STORE_PATH",
        "HGLOC_PUBLIC_DATASETS_JSON_KEY",
        "HGLOC_PUBLIC_DATASETS_ZIP_DIR_PREFIX"
    ]
    
    original_values = {var: os.environ.get(var) for var in env_vars_to_clear}

    for var in env_vars_to_clear:
        if var in os.environ:
            del os.environ[var]

    # If config.py uses dotenv, we need to ensure it doesn't load anything.
    # Patching 'dotenv.load_dotenv' might be tricky if it's already loaded by the time
    # this fixture runs for the first module import.
    # A more direct approach is to ensure that when config.py is loaded/reloaded,
    # the environment variables it looks for are simply not there.

    yield # Run tests

    # Restore original environment variables
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ: # If it was set to None but existed
             del os.environ[var]


# Fixture to provide mocker for session scope if needed (though usually function scope is fine)
@pytest.fixture(scope='session')
def session_mocker(request):
    from unittest.mock import patch as unpatch
    # This is a bit of a trick to get a "mocker" like object for session scope
    # For most cases, function-scoped mocker (the default pytest-mock fixture) is sufficient.
    # This specific implementation is simplified. For robust session mocking,
    # careful setup/teardown of patches is needed.
    # For now, let's rely on function-scoped `mocker` and ensure `manage_test_environment_variables`
    # directly manipulates os.environ, which affects subsequent imports/reloads.
    # The primary goal here is os.environ manipulation.
    pass


# This fixture will be autouse and module-scoped, ensuring that before
# any test module runs, we attempt to neutralize dotenv.
# This is critical if config.py calls load_dotenv() at module level.
@pytest.fixture(scope="module", autouse=True)
def mock_dotenv_for_module(module_mocker):
    """Patches dotenv.load_dotenv for the scope of a test module."""
    # Try to patch load_dotenv in hg_localization.config if it's imported there
    try:
        module_mocker.patch('hg_localization.config.load_dotenv', return_value=False)
    except AttributeError:
        # Fallback: Try to patch the more general 'dotenv.load_dotenv'
        # This might not always work if 'load_dotenv' is imported as 'from dotenv import load_dotenv'
        # and called directly, as the reference in config.py would be to the original.
        try:
            module_mocker.patch('dotenv.load_dotenv', return_value=False)
        except AttributeError:
            # dotenv might not be installed or used, or patching target is different
            pass
    yield


# The pytest-mock plugin provides the 'mocker' fixture which is function-scoped.
# We can use it directly in tests.
# The manage_test_environment_variables handles os.environ.
# The mock_dotenv_for_module attempts to neutralize dotenv at module load time. 