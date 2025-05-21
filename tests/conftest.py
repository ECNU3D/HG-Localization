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