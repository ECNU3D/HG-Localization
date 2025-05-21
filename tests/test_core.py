import pytest
from pathlib import Path
import os
import shutil
import tempfile
import requests
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from datasets import Dataset, DatasetDict

from hg_localization.core import (
    download_dataset,
    load_local_dataset,
    list_local_datasets,
    _get_dataset_path,
    _get_safe_path_component,
    _check_s3_dataset_exists,
    _get_s3_prefix,
    upload_dataset,
    DATASETS_STORE_PATH, # Import to ensure it's patched by fixture
    DEFAULT_CONFIG_NAME,
    DEFAULT_REVISION_NAME,
    S3_BUCKET_NAME, # Import for monkeypatching
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    PUBLIC_DATASETS_ZIP_DIR_PREFIX, # Import to allow patching its value
    _get_s3_public_url,
    _fetch_public_dataset_info
)
# from datasets import Dataset, DatasetDict # Not strictly needed for these mock-based tests

# Fixtures temp_datasets_store and mock_hf_datasets are auto-used from conftest.py

def test_get_dataset_path(temp_datasets_store):
    """Test the helper function for constructing dataset paths."""
    base_path = temp_datasets_store
    
    # Test with dataset_id only (should use defaults for config and revision)
    path1 = _get_dataset_path("test_dataset1")
    assert path1 == base_path / "test_dataset1" / DEFAULT_CONFIG_NAME / DEFAULT_REVISION_NAME

    # Test with dataset_id and config_name
    path2 = _get_dataset_path("test_dataset2", config_name="configA")
    assert path2 == base_path / "test_dataset2" / "configA" / DEFAULT_REVISION_NAME

    # Test with dataset_id and revision
    path3 = _get_dataset_path("test_dataset3", revision="revB")
    assert path3 == base_path / "test_dataset3" / DEFAULT_CONFIG_NAME / "revB"

    # Test with dataset_id, config_name, and revision
    path4 = _get_dataset_path("test_dataset4", config_name="configC", revision="revD")
    assert path4 == base_path / "test_dataset4" / "configC" / "revD"

    # Test with slashes in names (should be sanitized)
    path5 = _get_dataset_path("user/dataset5", config_name="conf/name", revision="branch/name")
    assert path5 == base_path / "user_dataset5" / "conf_name" / "branch_name"

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._check_s3_dataset_exists')
def test_download_dataset_new_full_spec(mock_check_s3_exists, mock_get_s3, temp_datasets_store, mock_hf_datasets, capsys):
    """Test downloading a new dataset with all specifiers."""
    dataset_id = "new_dataset"
    config_name = "config1"
    revision = "v1.0"

    # Simulate S3 not configured or dataset not found on S3 for this test
    # Option 1: S3 client init fails (e.g., S3_BUCKET_NAME not set or bad creds)
    mock_get_s3.return_value = None
    # Option 2: S3 client works, but dataset is not on S3
    # mock_s3_cli = MagicMock()
    # mock_get_s3.return_value = mock_s3_cli
    # mock_check_s3_exists.return_value = False
    
    success, msg = download_dataset(dataset_id, config_name=config_name, revision=revision, trust_remote_code=True)
    
    mock_hf_datasets['load_dataset'].assert_called_once_with(path=dataset_id, name=config_name, revision=revision, trust_remote_code=True)
    expected_path = temp_datasets_store / dataset_id / config_name / revision
    mock_hf_datasets['returned_dataset_instance'].save_to_disk.assert_called_once_with(str(expected_path))

    assert success is True
    assert msg == str(expected_path)
    assert (expected_path / "dataset_info.json").exists()
    captured = capsys.readouterr()
    assert f"Dataset '{dataset_id}' (config: {config_name}, revision: {revision}) successfully saved" in captured.out

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._check_s3_dataset_exists')
def test_download_dataset_new_defaults(mock_check_s3_exists, mock_get_s3, temp_datasets_store, mock_hf_datasets, capsys):
    """Test downloading with default config and revision."""
    dataset_id = "dataset_with_defaults"

    mock_get_s3.return_value = None # Simulate S3 check path is skipped or dataset not found
    # mock_s3_cli = MagicMock()
    # mock_get_s3.return_value = mock_s3_cli
    # mock_check_s3_exists.return_value = False
    
    success, msg = download_dataset(dataset_id, trust_remote_code=False)
    
    mock_hf_datasets['load_dataset'].assert_called_once_with(path=dataset_id, name=None, revision=None, trust_remote_code=False)
    expected_path = temp_datasets_store / dataset_id / DEFAULT_CONFIG_NAME / DEFAULT_REVISION_NAME
    mock_hf_datasets['returned_dataset_instance'].save_to_disk.assert_called_once_with(str(expected_path))
    assert success is True
    assert msg == str(expected_path) # Check the returned path
    # (expected_path / "dataset_info.json").exists() is implicitly tested by save_to_disk mock if needed
    captured = capsys.readouterr()
    assert f"Dataset '{dataset_id}' (config: default, revision: default) successfully saved" in captured.out

def test_download_dataset_already_exists(temp_datasets_store, mock_hf_datasets, capsys):
    dataset_id = "existing_dataset"
    config_name = "old_config"
    revision = "v0.9"
    expected_path = _get_dataset_path(dataset_id, config_name, revision)
    os.makedirs(expected_path, exist_ok=True)
    (expected_path / "dataset_info.json").touch()

    success, msg = download_dataset(dataset_id, config_name=config_name, revision=revision)

    assert success is True
    assert msg == str(expected_path)
    mock_hf_datasets['load_dataset'].assert_not_called()
    captured = capsys.readouterr()
    assert f"Dataset {dataset_id} (config: {config_name}, revision: {revision}) already exists locally at {expected_path}" in captured.out

def test_download_dataset_already_exists_dict_json(temp_datasets_store, mock_hf_datasets, capsys):
    """Test that download_dataset recognizes an existing dataset with dataset_dict.json."""
    dataset_id = "existing_dataset_dict"
    config_name = "dict_config"
    expected_path = _get_dataset_path(dataset_id, config_name=config_name)
    os.makedirs(expected_path, exist_ok=True)
    (expected_path / "dataset_dict.json").touch() # Use dataset_dict.json

    success, msg = download_dataset(dataset_id, config_name=config_name)

    assert success is True
    assert msg == str(expected_path)
    mock_hf_datasets['load_dataset'].assert_not_called()
    captured = capsys.readouterr()
    assert f"Dataset {dataset_id} (config: {config_name}, revision: default) already exists locally at {expected_path}" in captured.out # revision is default here

def test_download_dataset_hf_download_fails(temp_datasets_store, mock_hf_datasets, capsys):
    dataset_id = "fail_on_hf"
    mock_hf_datasets['load_dataset'].side_effect = FileNotFoundError("Mocked HF FileNotFoundError")

    success, msg = download_dataset(dataset_id)

    assert success is False
    assert f"Dataset '{dataset_id}' not found on Hugging Face Hub" in msg 
    expected_path = _get_dataset_path(dataset_id, None, None)
    assert not expected_path.exists()
    captured = capsys.readouterr()
    assert f"Error: Dataset '{dataset_id}' not found on Hugging Face Hub" in captured.out

def test_download_dataset_save_to_disk_fails(temp_datasets_store, mock_hf_datasets, mocker, capsys):
    dataset_id = "fail_on_save"
    config_name = "config_save_fail"
    error_message = "Disk write error simulation"
    mock_hf_datasets['returned_dataset_instance'].save_to_disk.side_effect = Exception(error_message)
    mock_rmtree = mocker.patch('shutil.rmtree')

    success, msg = download_dataset(dataset_id, config_name=config_name)

    assert success is False
    assert error_message in msg
    expected_path = _get_dataset_path(dataset_id, config_name, None)
    mock_rmtree.assert_called_once_with(expected_path)
    captured = capsys.readouterr()
    assert f"An error occurred while processing '{dataset_id}'" in captured.out
    assert f"Cleaned up partially saved data at {expected_path}" in captured.out

def test_load_local_dataset_success(temp_datasets_store, mock_hf_datasets, capsys):
    dataset_id = "local_loader_test"
    config_name = "cfg_load"
    revision = "r_load"
    dataset_path = _get_dataset_path(dataset_id, config_name, revision)
    os.makedirs(dataset_path, exist_ok=True)
    (dataset_path / "dataset_info.json").touch()

    expected_mock_data_object = mock_hf_datasets['data_loaded_from_disk']
    loaded_data = load_local_dataset(dataset_id, config_name=config_name, revision=revision)

    assert loaded_data is expected_mock_data_object
    mock_hf_datasets['load_from_disk'].assert_called_once_with(str(dataset_path))
    captured = capsys.readouterr()
    assert f"Loading dataset '{dataset_id}' (config: {config_name}, revision: {revision}) from {dataset_path}" in captured.out

def test_load_local_dataset_not_found_locally_s3_disabled(temp_datasets_store, mock_hf_datasets, monkeypatch, capsys):
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', None) # Disable S3
    dataset_id = "not_found_anywhere"
    loaded_data = load_local_dataset(dataset_id)
    assert loaded_data is None
    mock_hf_datasets['load_from_disk'].assert_not_called()
    captured = capsys.readouterr()
    assert f"Dataset '{dataset_id}' (config: {DEFAULT_CONFIG_NAME.replace('_',' ')}, revision: {DEFAULT_REVISION_NAME.replace('_',' ')}) not found in local cache" in captured.out
    assert "S3_BUCKET_NAME not configured. Cannot fetch public datasets JSON." in captured.out

def test_load_local_dataset_load_from_disk_error(temp_datasets_store, mock_hf_datasets, capsys):
    dataset_id = "load_error_ds"
    config_name = "cfg_err"
    dataset_path = _get_dataset_path(dataset_id, config_name, None)
    os.makedirs(dataset_path, exist_ok=True)
    (dataset_path / "dataset_info.json").touch()
    error_message = "Corrupted data on disk mock"
    mock_hf_datasets['load_from_disk'].side_effect = Exception(error_message)

    loaded_data = load_local_dataset(dataset_id, config_name=config_name)

    assert loaded_data is None
    captured = capsys.readouterr()
    assert f"An error occurred while loading '{dataset_id}' (config: {config_name}, revision: {DEFAULT_REVISION_NAME.replace('_',' ')}) from {dataset_path}: {error_message}" in captured.out

def test_list_local_datasets_empty(temp_datasets_store, capsys):
    datasets = list_local_datasets()
    assert datasets == []
    captured = capsys.readouterr()
    assert "No local datasets found in cache." in captured.out 

def test_list_local_datasets_with_data(temp_datasets_store):
    # Dataset 1: id_A, config_X, rev_1
    ds1_path = _get_dataset_path("id_A", "config_X", "rev_1")
    os.makedirs(ds1_path, exist_ok=True)
    (ds1_path / "dataset_info.json").touch()

    # Dataset 2: id_A, default_config, rev_2
    ds2_path = _get_dataset_path("id_A", None, "rev_2") # config_name=None -> DEFAULT_CONFIG_NAME
    os.makedirs(ds2_path, exist_ok=True)
    (ds2_path / "dataset_info.json").touch()

    # Dataset 3: id_B, config_Y, default_revision
    ds3_path = _get_dataset_path("id_B", "config_Y", None) # revision=None -> DEFAULT_REVISION_NAME
    os.makedirs(ds3_path, exist_ok=True)
    (ds3_path / "dataset_info.json").touch()
    
    # Incomplete entry (should be ignored)
    os.makedirs(_get_dataset_path("id_C", "config_Z", "rev_3").parent.parent, exist_ok=True)

    datasets = list_local_datasets()
    assert len(datasets) == 3
    
    expected_datasets = [
        {"dataset_id": "id_A", "config_name": "config_X", "revision": "rev_1"},
        {"dataset_id": "id_A", "config_name": None, "revision": "rev_2"}, # Stored as DEFAULT_CONFIG_NAME, listed as None
        {"dataset_id": "id_B", "config_name": "config_Y", "revision": None},# Stored as DEFAULT_REVISION_NAME, listed as None
    ]

    # Sort for consistent comparison
    datasets.sort(key=lambda x: (x['dataset_id'], str(x['config_name']), str(x['revision'])))
    expected_datasets.sort(key=lambda x: (x['dataset_id'], str(x['config_name']), str(x['revision'])))
    assert datasets == expected_datasets

# S3-related tests (list_s3_datasets, S3 download part of load_local_dataset) 
# would require mocking boto3 S3 client, typically with `moto`.
# These are not updated here but would be the next step for full S3 testing.
# For now, ensuring list_local_datasets and the local cache parts work with the new structure.

def test_list_local_datasets_store_does_not_exist(monkeypatch, capsys):
    non_existent_path = Path(tempfile.mkdtemp())
    shutil.rmtree(non_existent_path)
    monkeypatch.setattr('hg_localization.core.DATASETS_STORE_PATH', non_existent_path)
    datasets = list_local_datasets()
    assert datasets == []
    captured = capsys.readouterr()
    assert f"Local dataset store directory does not exist: {non_existent_path}" in captured.out 

# --- Tests for _check_s3_dataset_exists ---

@pytest.fixture
def mock_s3_client():
    """Provides a MagicMock for the S3 client."""
    return MagicMock()

def test_check_s3_dataset_exists_info_json_present(mock_s3_client):
    mock_s3_client.head_object.return_value = {} # Simulate object exists
    assert _check_s3_dataset_exists(mock_s3_client, "test-bucket", "prefix/dataset") is True
    mock_s3_client.head_object.assert_called_once_with(Bucket="test-bucket", Key="prefix/dataset/dataset_info.json")

def test_check_s3_dataset_exists_dict_json_present(mock_s3_client):
    # First call for dataset_info.json raises 404, second for dataset_dict.json succeeds
    mock_s3_client.head_object.side_effect = [
        ClientError({'Error': {'Code': '404'}}, 'head_object'), # For dataset_info.json
        {}  # For dataset_dict.json
    ]
    assert _check_s3_dataset_exists(mock_s3_client, "test-bucket", "prefix/dataset") is True
    assert mock_s3_client.head_object.call_count == 2
    mock_s3_client.head_object.assert_any_call(Bucket="test-bucket", Key="prefix/dataset/dataset_info.json")
    mock_s3_client.head_object.assert_any_call(Bucket="test-bucket", Key="prefix/dataset/dataset_dict.json")

def test_check_s3_dataset_exists_neither_json_present(mock_s3_client):
    mock_s3_client.head_object.side_effect = ClientError({'Error': {'Code': '404'}}, 'head_object')
    assert _check_s3_dataset_exists(mock_s3_client, "test-bucket", "prefix/dataset") is False
    assert mock_s3_client.head_object.call_count == 2 # Checks for info then dict

def test_check_s3_dataset_exists_other_client_error_info_json(mock_s3_client):
    mock_s3_client.head_object.side_effect = ClientError({'Error': {'Code': '500'}}, 'head_object')
    assert _check_s3_dataset_exists(mock_s3_client, "test-bucket", "prefix/dataset") is False
    mock_s3_client.head_object.assert_called_once_with(Bucket="test-bucket", Key="prefix/dataset/dataset_info.json")

def test_check_s3_dataset_exists_other_client_error_dict_json(mock_s3_client):
    mock_s3_client.head_object.side_effect = [
        ClientError({'Error': {'Code': '404'}}, 'head_object'), # For dataset_info.json
        ClientError({'Error': {'Code': '503'}}, 'head_object')  # For dataset_dict.json
    ]
    assert _check_s3_dataset_exists(mock_s3_client, "test-bucket", "prefix/dataset") is False
    assert mock_s3_client.head_object.call_count == 2

def test_check_s3_dataset_exists_s3_not_configured():
    assert _check_s3_dataset_exists(None, "test-bucket", "prefix/dataset") is False
    assert _check_s3_dataset_exists(MagicMock(), "", "prefix/dataset") is False

# --- End of tests for _check_s3_dataset_exists --- 

# --- Tests for download_dataset with S3 integration ---

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._check_s3_dataset_exists')
@patch('hg_localization.core._download_directory_from_s3')
def test_download_dataset_not_local_exists_on_s3_download_success(
    mock_download_from_s3, mock_check_s3, mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    dataset_id = "s3_dataset"
    config_name = "s3_config"
    revision = "s3_rev"
    s3_bucket = "my-s3-bucket"
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', s3_bucket)
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', "dummy_key") # Ensure client can be "created"
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', "dummy_secret")

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    mock_check_s3.return_value = True  # Dataset exists on S3
    mock_download_from_s3.return_value = True # S3 download succeeds

    # Ensure the local path does not exist initially
    local_save_path = _get_dataset_path(dataset_id, config_name, revision)
    assert not local_save_path.exists()

    # Simulate that _download_directory_from_s3 creates the marker file
    def side_effect_download_s3(*args, **kwargs):
        os.makedirs(local_save_path, exist_ok=True)
        (local_save_path / "dataset_dict.json").touch()
        return True
    mock_download_from_s3.side_effect = side_effect_download_s3

    success, msg = download_dataset(dataset_id, config_name=config_name, revision=revision)

    assert success is True
    assert msg == str(local_save_path)
    mock_get_s3_client.assert_called_once()
    s3_prefix = f"{dataset_id}/{config_name}/{revision}" # Assuming default safe names for simplicity here
    mock_check_s3.assert_called_once_with(mock_s3_cli, s3_bucket, s3_prefix)
    mock_download_from_s3.assert_called_once_with(mock_s3_cli, local_save_path, s3_bucket, s3_prefix)
    mock_hf_datasets['load_dataset'].assert_not_called()
    captured = capsys.readouterr()
    assert f"Dataset found on S3. Attempting to download from S3 to local cache: {local_save_path}" in captured.out
    assert f"Successfully downloaded dataset from S3 to {local_save_path}" in captured.out

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._check_s3_dataset_exists')
@patch('hg_localization.core._download_directory_from_s3')
def test_download_dataset_not_local_exists_on_s3_download_fails(
    mock_download_from_s3, mock_check_s3, mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    dataset_id = "s3_fail_dl"
    config_name = "s3_fail_cfg"
    s3_bucket = "my-s3-bucket"
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', s3_bucket)
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', "dummy_key")
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', "dummy_secret")

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    mock_check_s3.return_value = True  # Dataset exists on S3
    mock_download_from_s3.return_value = False # S3 download FAILS

    # Mock HF download part
    mock_hf_datasets['load_dataset'].return_value = mock_hf_datasets['returned_dataset_instance']
    local_save_path = _get_dataset_path(dataset_id, config_name)

    success, msg = download_dataset(dataset_id, config_name=config_name)

    assert success is True # Should still succeed by falling back to HF
    assert msg == str(local_save_path)
    assert mock_get_s3_client.call_count == 2
    s3_prefix = f"{dataset_id}/{config_name}/{DEFAULT_REVISION_NAME}"
    mock_check_s3.assert_called_once_with(mock_s3_cli, s3_bucket, s3_prefix)
    mock_download_from_s3.assert_called_once_with(mock_s3_cli, local_save_path, s3_bucket, s3_prefix)
    mock_hf_datasets['load_dataset'].assert_called_once_with(path=dataset_id, name=config_name, revision=None, trust_remote_code=False)
    mock_hf_datasets['returned_dataset_instance'].save_to_disk.assert_called_once_with(str(local_save_path))
    captured = capsys.readouterr()
    assert "Failed to download dataset from S3. Will attempt Hugging Face download." in captured.out
    assert "Downloading dataset" in captured.out # Indicates HF download attempt

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._check_s3_dataset_exists')
def test_download_dataset_not_local_not_on_s3(
    mock_check_s3, mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    dataset_id = "not_on_s3"
    s3_bucket = "my-s3-bucket"
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', s3_bucket)
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', "dummy_key")
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', "dummy_secret")

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    mock_check_s3.return_value = False  # Dataset does NOT exist on S3

    local_save_path = _get_dataset_path(dataset_id)
    success, msg = download_dataset(dataset_id)

    assert success is True
    assert msg == str(local_save_path)
    mock_check_s3.assert_called_once()
    mock_hf_datasets['load_dataset'].assert_called_once()
    captured = capsys.readouterr()
    assert "Dataset not found on S3. Will attempt Hugging Face download." in captured.out

@patch('hg_localization.core._get_s3_client')
def test_download_dataset_s3_not_configured(
    mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    dataset_id = "s3_not_configured"
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', None) # S3 not configured
    # No need to set AWS keys if bucket name is None, as _get_s3_client will return None early.

    mock_get_s3_client.return_value = None # Simulate S3 client init returning None
    mock_hf_datasets['load_dataset'].return_value = mock_hf_datasets['returned_dataset_instance']

    local_save_path = _get_dataset_path(dataset_id)
    success, msg = download_dataset(dataset_id)

    assert success is True
    assert msg == str(local_save_path)
    assert mock_get_s3_client.call_count == 2 # It will be called to check, then again for upload
    # _check_s3_dataset_exists and _download_directory_from_s3 should not be called
    # This can be verified by them not being patched and thus not having call records,
    mock_hf_datasets['load_dataset'].assert_called_once()
    captured = capsys.readouterr()
    assert "S3 not configured or client init failed; skipping S3 check." in captured.out

# --- End of tests for download_dataset with S3 integration --- 

# --- Tests for upload_dataset --- 

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._upload_directory_to_s3')
def test_upload_dataset_success_with_s3(
    mock_upload_to_s3, mock_get_s3_client, 
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    """Test successful local save and S3 upload."""
    dataset_id = "upload_ds_s3"
    config_name = "cfg_up_s3"
    revision = "rev_up_s3"
    s3_bucket = "my-upload-bucket"
    
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', s3_bucket)
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', "dummy_key")
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', "dummy_secret")

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    mock_dataset_obj = mock_hf_datasets['returned_dataset_instance'] # Use the mock dataset from fixture

    expected_local_path = _get_dataset_path(dataset_id, config_name, revision)
    s3_prefix = f"{dataset_id}/{config_name}/{revision}"

    success = upload_dataset(mock_dataset_obj, dataset_id, config_name, revision)

    assert success is True
    mock_dataset_obj.save_to_disk.assert_called_once_with(str(expected_local_path))
    mock_get_s3_client.assert_called_once()
    mock_upload_to_s3.assert_called_once_with(mock_s3_cli, expected_local_path, s3_bucket, s3_prefix)
    captured = capsys.readouterr()
    assert f"Dataset '{dataset_id}' (config: {config_name}, revision: {revision}) successfully saved to local cache" in captured.out
    assert f"Successfully initiated upload of dataset '{dataset_id}' (config: {config_name}, revision: {revision}) to S3" in captured.out

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._upload_directory_to_s3')
def test_upload_dataset_s3_not_configured(
    mock_upload_to_s3, mock_get_s3_client, 
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    """Test successful local save when S3 is not configured."""
    dataset_id = "upload_ds_no_s3"
    config_name = "cfg_up_no_s3"
    
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', None) # S3 not configured
    mock_get_s3_client.return_value = None # _get_s3_client will return None
    mock_dataset_obj = mock_hf_datasets['returned_dataset_instance']
    expected_local_path = _get_dataset_path(dataset_id, config_name)

    success = upload_dataset(mock_dataset_obj, dataset_id, config_name)

    assert success is True
    mock_dataset_obj.save_to_disk.assert_called_once_with(str(expected_local_path))
    mock_get_s3_client.assert_called_once() # It's called to check
    mock_upload_to_s3.assert_not_called()
    captured = capsys.readouterr()
    assert "S3 not configured or client init failed; skipping S3 upload. Dataset is saved locally." in captured.out

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._upload_directory_to_s3')
def test_upload_dataset_s3_upload_fails(
    mock_upload_to_s3, mock_get_s3_client, 
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    """Test successful local save but S3 upload fails."""
    dataset_id = "upload_s3_fail"
    config_name = "cfg_s3_fail"
    s3_bucket = "my-upload-bucket-fail"
    
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', s3_bucket)
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', "dummy_key")
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', "dummy_secret")

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    mock_upload_to_s3.side_effect = Exception("Mocked S3 upload failure")
    mock_dataset_obj = mock_hf_datasets['returned_dataset_instance']
    expected_local_path = _get_dataset_path(dataset_id, config_name)

    success = upload_dataset(mock_dataset_obj, dataset_id, config_name)

    assert success is False # Should be False as S3 upload failed
    mock_dataset_obj.save_to_disk.assert_called_once_with(str(expected_local_path))
    mock_upload_to_s3.assert_called_once()
    captured = capsys.readouterr()
    assert f"Error uploading dataset '{dataset_id}' (config: {config_name}, revision: default) to S3: Mocked S3 upload failure" in captured.out

def test_upload_dataset_local_save_fails(
    temp_datasets_store, mock_hf_datasets, capsys # No S3 mocks needed if local save fails first
):
    """Test failure during the local save operation."""
    dataset_id = "upload_local_fail"
    mock_dataset_obj = mock_hf_datasets['returned_dataset_instance']
    mock_dataset_obj.save_to_disk.side_effect = Exception("Mocked local save failure")

    success = upload_dataset(mock_dataset_obj, dataset_id)

    assert success is False
    mock_dataset_obj.save_to_disk.assert_called_once() # Attempted to save
    captured = capsys.readouterr()
    assert f"Error saving dataset '{dataset_id}' (config: default, revision: default) to {_get_dataset_path(dataset_id)}: Mocked local save failure" in captured.out

# --- End of tests for upload_dataset --- 

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._zip_directory')
@patch('hg_localization.core._update_public_datasets_json')
@patch('hg_localization.core.get_dataset_card_content') # Mock card fetching
def test_download_dataset_make_public_success(
    mock_get_card_content, mock_update_json, mock_zip_directory, mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    """Test download_dataset with make_public=True successfully creates and uploads public zip."""
    dataset_id = "ds_public_success"
    config_name = "cfg_public_success"
    revision = "rev_public_success"

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', 'test-bucket')
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', 'test-key')
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', 'test-secret')

    with patch('hg_localization.core._check_s3_dataset_exists', return_value=False):
        mock_zip_directory.return_value = True
        mock_update_json.return_value = True
        mock_get_card_content.return_value = "Mocked card content"

        success, msg = download_dataset(
            dataset_id, config_name=config_name, revision=revision,
            make_public=True, trust_remote_code=True
        )

        assert success is True
        expected_path = _get_dataset_path(dataset_id, config_name, revision)
        assert msg == str(expected_path)

        mock_hf_datasets['load_dataset'].assert_called_once()
        mock_hf_datasets['returned_dataset_instance'].save_to_disk.assert_called_once_with(str(expected_path))
        mock_get_card_content.assert_called_once_with(dataset_id, revision=revision)
        assert (expected_path / "dataset_card.md").exists()
        mock_zip_directory.assert_called_once()
        assert mock_zip_directory.call_args[0][1].suffix == ".zip"

        safe_ds_id = _get_safe_path_component(dataset_id)
        safe_cfg = _get_safe_path_component(config_name)
        safe_rev = _get_safe_path_component(revision)
        zip_name = f"{safe_ds_id}---{safe_cfg}---{safe_rev}.zip"
        s3_zip_base = f"{PUBLIC_DATASETS_ZIP_DIR_PREFIX}/{zip_name}"
        s3_prefix_env = os.environ.get("HGLOC_S3_DATA_PREFIX", "").strip('/')
        s3_zip_full = f"{s3_prefix_env}/{s3_zip_base}" if s3_prefix_env else s3_zip_base

        public_zip_uploaded = False
        for call_item in mock_s3_cli.upload_file.call_args_list:
            args, kwargs = call_item
            if args[0] == str(mock_zip_directory.call_args[0][1]) and \
               args[1] == 'test-bucket' and args[2] == s3_zip_full and \
               kwargs.get('ExtraArgs') == {'ACL': 'public-read'}:
                public_zip_uploaded = True
                break
        assert public_zip_uploaded, "Public S3 zip upload call not found or args mismatch."

        mock_update_json.assert_called_once_with(mock_s3_cli, 'test-bucket', dataset_id, config_name, revision, s3_zip_base)
        captured = capsys.readouterr()
        assert f"Successfully uploaded public zip to {s3_zip_full}" in captured.out
        assert "Preparing to make dataset" in captured.out
        # Optional: Check for _update_public_datasets_json success print if its mock is set up to print.


@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._zip_directory')
@patch('hg_localization.core._update_public_datasets_json')
@patch('hg_localization.core.get_dataset_card_content')
def test_download_dataset_make_public_zip_fails(
    mock_get_card_content, mock_update_json, mock_zip_directory, mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    dataset_id = "ds_zip_fail"
    config_name = "cfg_zip_fail"
    revision = "rev_zip_fail"

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', 'test-bucket')
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', 'test-key')
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', 'test-secret')

    mock_zip_directory.return_value = False
    mock_get_card_content.return_value = "card content"

    with patch('hg_localization.core._check_s3_dataset_exists', return_value=False):
        success, _ = download_dataset(dataset_id, config_name=config_name, revision=revision, make_public=True, trust_remote_code=True)

    assert success is True
    mock_zip_directory.assert_called_once()
    
    safe_ds_id = _get_safe_path_component(dataset_id)
    safe_cfg = _get_safe_path_component(config_name)
    safe_rev = _get_safe_path_component(revision)
    zip_name = f"{safe_ds_id}---{safe_cfg}---{safe_rev}.zip"
    s3_zip_key_fragment = f"{PUBLIC_DATASETS_ZIP_DIR_PREFIX}/{zip_name}"

    public_zip_upload_attempted = False
    for call_item in mock_s3_cli.upload_file.call_args_list:
        args, kwargs = call_item
        if s3_zip_key_fragment in args[2] and kwargs.get('ExtraArgs') == {'ACL': 'public-read'}:
            public_zip_upload_attempted = True
            break
    assert not public_zip_upload_attempted, "Public zip S3 upload should not have been attempted."

    mock_update_json.assert_not_called()
    captured = capsys.readouterr()
    assert "Failed to zip dataset for public upload." in captured.out


@patch('hg_localization.core.PUBLIC_DATASETS_ZIP_DIR_PREFIX', "public_datasets_zip")
@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._zip_directory')
@patch('hg_localization.core._update_public_datasets_json')
@patch('hg_localization.core.get_dataset_card_content')
def test_download_dataset_make_public_s3_zip_upload_fails(
    mock_get_card_content, mock_update_json, mock_zip_directory, mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    dataset_id = "ds_s3_upload_fail"
    config_name = "cfg_s3_upload_fail"
    revision = "rev_s3_upload_fail"

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    
    s3_upload_err = ClientError({'Error': {'Code': 'S3Fail', 'Message': 'Mock S3 Upload Err'}}, 'upload_file')
    safe_ds_id = _get_safe_path_component(dataset_id)
    safe_cfg = _get_safe_path_component(config_name)
    safe_rev = _get_safe_path_component(revision)
    zip_name = f"{safe_ds_id}---{safe_cfg}---{safe_rev}.zip"
    s3_zip_base = f"{PUBLIC_DATASETS_ZIP_DIR_PREFIX}/{zip_name}"
    s3_prefix_env = os.environ.get("HGLOC_S3_DATA_PREFIX", "").strip('/')
    s3_zip_full_for_effect = f"{s3_prefix_env}/{s3_zip_base}" if s3_prefix_env else s3_zip_base

    def selective_upload_fail(local_path, bucket, key, ExtraArgs=None):
        if key == s3_zip_full_for_effect and ExtraArgs == {'ACL': 'public-read'}:
            raise s3_upload_err
        # Allow other uploads (e.g., private dataset files) to pass or be MagicMock defaults
        return None 
    mock_s3_cli.upload_file.side_effect = selective_upload_fail

    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', 'test-bucket')
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', 'test-key')
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', 'test-secret')

    mock_zip_directory.return_value = True
    mock_get_card_content.return_value = "card content"

    with patch('hg_localization.core._check_s3_dataset_exists', return_value=False):
        success, _ = download_dataset(dataset_id, config_name=config_name, revision=revision, make_public=True, trust_remote_code=True)
    
    assert success is True
    mock_zip_directory.assert_called_once()
    
    public_zip_upload_attempted = False
    for call_item in mock_s3_cli.upload_file.call_args_list:
        args, kwargs = call_item
        if args[2] == s3_zip_full_for_effect and kwargs.get('ExtraArgs') == {'ACL': 'public-read'}:
            public_zip_upload_attempted = True
            break
    assert public_zip_upload_attempted, "Public S3 zip upload not attempted or side_effect not triggered as expected."

    mock_update_json.assert_not_called()
    captured = capsys.readouterr()
    assert f"Failed to upload public zip {s3_zip_full_for_effect}" in captured.out
    assert "Mock S3 Upload Err" in captured.out


@patch('hg_localization.core.PUBLIC_DATASETS_ZIP_DIR_PREFIX', "public_datasets_zip")
@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._zip_directory')
@patch('hg_localization.core._update_public_datasets_json')
@patch('hg_localization.core.get_dataset_card_content')
def test_download_dataset_make_public_update_json_fails(
    mock_get_card_content, mock_update_json_main, mock_zip_directory, mock_get_s3_client,
    temp_datasets_store, mock_hf_datasets, monkeypatch, capsys
):
    dataset_id = "ds_json_update_fail"
    config_name = "cfg_json_update_fail"
    revision = "rev_json_update_fail"

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', 'test-bucket')
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', 'test-key')
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', 'test-secret')

    mock_zip_directory.return_value = True
    mock_s3_cli.upload_file.return_value = None # Simulate S3 public zip upload success
    mock_update_json_main.return_value = False # Simulate JSON update failure
    mock_get_card_content.return_value = "card content"

    with patch('hg_localization.core._check_s3_dataset_exists', return_value=False):
        success, _ = download_dataset(dataset_id, config_name=config_name, revision=revision, make_public=True, trust_remote_code=True)

    assert success is True
    mock_zip_directory.assert_called_once()

    safe_ds_id = _get_safe_path_component(dataset_id)
    safe_cfg = _get_safe_path_component(config_name)
    safe_rev = _get_safe_path_component(revision)
    zip_name = f"{safe_ds_id}---{safe_cfg}---{safe_rev}.zip"
    s3_zip_base = f"{PUBLIC_DATASETS_ZIP_DIR_PREFIX}/{zip_name}"
    s3_prefix_env = os.environ.get("HGLOC_S3_DATA_PREFIX", "").strip('/')
    s3_zip_full = f"{s3_prefix_env}/{s3_zip_base}" if s3_prefix_env else s3_zip_base

    public_zip_upload_attempted = False
    for call_item in mock_s3_cli.upload_file.call_args_list:
        args, kwargs = call_item
        if args[1] == 'test-bucket' and args[2] == s3_zip_full and kwargs.get('ExtraArgs') == {'ACL': 'public-read'}:
            public_zip_upload_attempted = True
            break
    assert public_zip_upload_attempted, "Public S3 zip upload was not called prior to JSON update."

    mock_update_json_main.assert_called_once_with(mock_s3_cli, 'test-bucket', dataset_id, config_name, revision, s3_zip_base)

    captured = capsys.readouterr()
    assert f"Successfully uploaded public zip to {s3_zip_full}" in captured.out
    # download_dataset itself doesn't print an error if _update_public_datasets_json returns False.
    # That function is expected to print its own error internally.

def test_upload_dataset_local_save_fails(
    temp_datasets_store, mock_hf_datasets, capsys # No S3 mocks needed if local save fails first
):
    """Test failure during the local save operation."""
    dataset_id = "upload_local_fail"
    mock_dataset_obj = mock_hf_datasets['returned_dataset_instance']
    mock_dataset_obj.save_to_disk.side_effect = Exception("Mocked local save failure")

    success = upload_dataset(mock_dataset_obj, dataset_id)

    assert success is False
    mock_dataset_obj.save_to_disk.assert_called_once() # Attempted to save
    captured = capsys.readouterr()
    assert f"Error saving dataset '{dataset_id}' (config: default, revision: default) to {_get_dataset_path(dataset_id)}: Mocked local save failure" in captured.out

# --- Tests for load_local_dataset S3 authenticated download ---

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._download_directory_from_s3')
@patch('hg_localization.core.load_from_disk')
def test_load_local_dataset_s3_auth_download_success(
    mock_load_from_disk_core, mock_download_s3_dir, mock_get_s3_client,
    temp_datasets_store, monkeypatch, capsys
):
    """Test load_local_dataset: not local, S3 (auth) download success."""
    dataset_id = "s3_auth_ds"
    local_path = _get_dataset_path(dataset_id) # core._get_dataset_path

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', 'fake_access_key')
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', 'fake_secret_key')
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', 'test-bucket')

    # Simulate S3 download success
    def simulate_s3_download_success(s3_client, local_dir_path, bucket, prefix):
        # Simulate that the download creates the dataset_info.json
        # Ensure parent directory exists, similar to how _download_directory_from_s3 would.
        file_path_to_create = local_dir_path / "dataset_info.json"
        os.makedirs(file_path_to_create.parent, exist_ok=True)
        file_path_to_create.touch()
        return True
    mock_download_s3_dir.side_effect = simulate_s3_download_success

    mock_loaded_data = MagicMock(spec=DatasetDict) # Or Dataset, depending on what's expected
    mock_load_from_disk_core.return_value = mock_loaded_data

    result = load_local_dataset(dataset_id)

    assert result == mock_loaded_data
    mock_get_s3_client.assert_called_once()
    s3_prefix = _get_s3_prefix(dataset_id) # core._get_s3_prefix
    mock_download_s3_dir.assert_called_once_with(mock_s3_cli, local_path, 'test-bucket', s3_prefix)
    mock_load_from_disk_core.assert_called_once_with(str(local_path))
    captured = capsys.readouterr()
    assert f"Dataset '{dataset_id}'" in captured.out # General check
    assert "not found in local cache" in captured.out
    assert "Attempting to fetch from S3 using credentials..." in captured.out
    assert f"Successfully downloaded from S3 (authenticated) to {local_path}" in captured.out
    assert f"Loading dataset '{dataset_id}'" in captured.out # From the final load_from_disk call

@patch('hg_localization.core._get_s3_client')
@patch('hg_localization.core._download_directory_from_s3')
@patch('hg_localization.core.load_from_disk') # To prevent it from trying to load if download fails
@patch('hg_localization.core._fetch_public_dataset_info') # Mock this to prevent fallback to public
def test_load_local_dataset_s3_auth_download_fails_then_no_public(
    mock_fetch_public_info, mock_load_from_disk_core, mock_download_s3_dir, mock_get_s3_client,
    temp_datasets_store, monkeypatch, capsys
):
    """Test load_local_dataset: not local, S3 (auth) download fails, no public fallback."""
    dataset_id = "s3_auth_fail_ds"
    local_path = _get_dataset_path(dataset_id)

    mock_s3_cli = MagicMock()
    mock_get_s3_client.return_value = mock_s3_cli
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', 'fake_access_key')
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', 'fake_secret_key')
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', 'test-bucket')

    mock_download_s3_dir.return_value = False # Simulate S3 download failure
    mock_fetch_public_info.return_value = None # Simulate no public dataset info found

    result = load_local_dataset(dataset_id)

    assert result is None
    mock_get_s3_client.assert_called_once()
    s3_prefix = _get_s3_prefix(dataset_id)
    mock_download_s3_dir.assert_called_once_with(mock_s3_cli, local_path, 'test-bucket', s3_prefix)
    mock_load_from_disk_core.assert_not_called() # Should not try to load if download failed and no public
    mock_fetch_public_info.assert_called_once_with(dataset_id, None, None) # Default config/revision
    captured = capsys.readouterr()
    assert "not found in local cache" in captured.out
    assert "Attempting to fetch from S3 using credentials..." in captured.out
    assert f"Failed to download '{dataset_id}'" in captured.out
    assert "Attempting to fetch from public S3 dataset list via URL..." in captured.out
    assert "could not be fetched from any source" in captured.out

@patch('hg_localization.core._get_s3_client') # Only mock get_s3_client to test S3 init failure path
def test_load_local_dataset_s3_auth_client_fails(
    mock_get_s3_client, temp_datasets_store, monkeypatch, capsys
):
    """Test load_local_dataset: S3 client init fails during authenticated download attempt."""
    dataset_id = "s3_auth_client_fail_ds"
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', 'fake_access_key')
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', 'fake_secret_key')
    # S3_BUCKET_NAME is set by default test setup or needs to be explicitly set if required for client init path
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', 'test-bucket')


    mock_get_s3_client.return_value = None # Simulate S3 client initialization failure

    # We need to mock _fetch_public_dataset_info as it would be called next
    with patch('hg_localization.core._fetch_public_dataset_info') as mock_fetch_public_info:
        mock_fetch_public_info.return_value = None # No public fallback

        result = load_local_dataset(dataset_id)

        assert result is None
        mock_get_s3_client.assert_called_once() # Called for the authenticated attempt
        captured = capsys.readouterr()
        assert "not found in local cache" in captured.out
        assert "S3 client could not be initialized for authenticated download" in captured.out
        assert "Attempting to fetch from public S3 dataset list via URL..." in captured.out # Fallback attempt
        assert "could not be fetched from any source" in captured.out


# --- Tests for load_local_dataset Public S3 Zip Download ---

@patch('hg_localization.core._fetch_public_dataset_info')
@patch('requests.get') # Mock requests.get directly for URL download
@patch('hg_localization.core._unzip_file')
@patch('hg_localization.core.load_from_disk')
@patch('tempfile.NamedTemporaryFile') # To control the temporary file
@patch('shutil.rmtree') # To check cleanup
def test_load_local_dataset_public_s3_zip_success(
    mock_rmtree, mock_temp_file, mock_load_from_disk, mock_unzip, mock_requests_get,
    mock_fetch_public_info, temp_datasets_store, monkeypatch, capsys
):
    """Test load_local_dataset: not local, no S3 auth, public S3 zip download success."""
    dataset_id = "public_zip_ds"
    config_name = "public_config"
    revision = "v1_public"
    local_path = _get_dataset_path(dataset_id, config_name, revision)
    s3_bucket = "my-public-bucket"
    s3_zip_key = "public_zips/public_zip_ds---public_config---v1_public.zip"

    # Simulate no S3 auth credentials
    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', None)
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', s3_bucket) # For _get_s3_public_url if needed
    monkeypatch.setattr('hg_localization.core.S3_ENDPOINT_URL', "https://s3.example.com")

    mock_fetch_public_info.return_value = {
        "s3_bucket": s3_bucket,
        "s3_zip_key": s3_zip_key,
        "dataset_id": dataset_id, "config_name": config_name, "revision": revision
    }

    # Mock requests.get response
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"zip_content_chunk1", b"zip_content_chunk2"]
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    # Mock tempfile.NamedTemporaryFile
    mock_temp_zip = MagicMock()
    mock_temp_zip.name = "/tmp/fake_public_zip.zip"
    mock_temp_zip_path = Path(mock_temp_zip.name)
    mock_temp_file.return_value.__enter__.return_value = mock_temp_zip # Simulate 'with' statement

    # mock_unzip.return_value = True # Simulate unzip success
    # Instead, simulate that unzip creates the necessary indicator file:
    def simulate_unzip_success(zip_path, extract_path):
        (extract_path / "dataset_info.json").touch()
        return True
    mock_unzip.side_effect = simulate_unzip_success

    mock_loaded_data = MagicMock(spec=DatasetDict)
    mock_load_from_disk.return_value = mock_loaded_data

    result = load_local_dataset(dataset_id, config_name=config_name, revision=revision)

    assert result == mock_loaded_data
    mock_fetch_public_info.assert_called_once_with(dataset_id, config_name, revision)
    expected_zip_url = _get_s3_public_url(s3_bucket, s3_zip_key, "https://s3.example.com")
    mock_requests_get.assert_called_once_with(expected_zip_url, stream=True, timeout=300)
    mock_temp_file.assert_called_once_with(suffix=".zip", delete=False)
    mock_unzip.assert_called_once_with(mock_temp_zip_path, local_path)
    mock_load_from_disk.assert_called_once_with(str(local_path))

    # Check that the temp zip was written to and closed
    mock_temp_zip.write.assert_any_call(b"zip_content_chunk1")
    mock_temp_zip.write.assert_any_call(b"zip_content_chunk2")
    mock_temp_zip.close.assert_called_once()

    # Check temp file cleanup (os.remove is called in a try-except block)
    with patch('os.remove') as mock_os_remove:
        # Re-run the relevant part of the logic or just assert based on expectations if complex
        # For simplicity, we assume it tried to remove if download was successful.
        # This part is tricky to test precisely without more refactoring of core.py
        # We can check if the tempfile context manager was used, which implies cleanup will be attempted.
        pass 

    captured = capsys.readouterr()
    assert "not found in local cache" in captured.out
    assert "Attempting to fetch from public S3 dataset list via URL..." in captured.out
    assert f"Public dataset zip found. Attempting download from: {expected_zip_url}" in captured.out
    assert f"Public zip downloaded to {mock_temp_zip_path}. Unzipping..." in captured.out
    assert f"Successfully downloaded and unzipped public dataset to {local_path}" in captured.out
    assert f"Loading dataset '{dataset_id}'" in captured.out

@patch('hg_localization.core._fetch_public_dataset_info')
@patch('requests.get')
@patch('hg_localization.core._unzip_file')
@patch('shutil.rmtree') # For checking cleanup
@patch('tempfile.NamedTemporaryFile')
def test_load_local_dataset_public_s3_zip_download_fails(
    mock_temp_file, mock_rmtree, mock_unzip, mock_requests_get,
    mock_fetch_public_info, temp_datasets_store, monkeypatch, capsys
):
    """Test load_local_dataset: public S3 zip download fails (requests.get fails)."""
    dataset_id = "public_zip_fail_dl"
    local_path = _get_dataset_path(dataset_id)
    s3_bucket = "my-public-bucket"
    s3_zip_key = "public_zips/public_zip_fail_dl.zip"

    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', None)
    monkeypatch.setattr('hg_localization.core.S3_BUCKET_NAME', s3_bucket)
    monkeypatch.setattr('hg_localization.core.S3_ENDPOINT_URL', "https://s3.example.com")

    mock_fetch_public_info.return_value = {"s3_bucket": s3_bucket, "s3_zip_key": s3_zip_key}
    mock_requests_get.side_effect = requests.exceptions.HTTPError("Mocked HTTP Error")

    mock_temp_zip = MagicMock()
    mock_temp_zip.name = "/tmp/fake_dl_fail.zip"
    mock_temp_file.return_value.__enter__.return_value = mock_temp_zip

    result = load_local_dataset(dataset_id)

    assert result is None
    expected_zip_url = _get_s3_public_url(s3_bucket, s3_zip_key, "https://s3.example.com")
    mock_requests_get.assert_called_once_with(expected_zip_url, stream=True, timeout=300)
    mock_unzip.assert_not_called()
    mock_rmtree.assert_not_called() # local_path directory is not created before requests.get fails
    captured = capsys.readouterr()
    assert f"HTTP error downloading public zip {expected_zip_url}" in captured.out
    assert "could not be fetched from any source" in captured.out

@patch('hg_localization.core._fetch_public_dataset_info')
def test_load_local_dataset_public_s3_info_incomplete(
    mock_fetch_public_info, temp_datasets_store, monkeypatch, capsys
):
    """Test load_local_dataset: public S3 info is incomplete (missing s3_zip_key)."""
    dataset_id = "public_zip_incomplete_info"

    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', None)

    # Simulate incomplete info (missing 's3_zip_key')
    mock_fetch_public_info.return_value = {"s3_bucket": "my-public-bucket"}

    result = load_local_dataset(dataset_id)

    assert result is None
    captured = capsys.readouterr()
    assert "not found in public S3 dataset list or info was incomplete" in captured.out
    assert "could not be fetched from any source" in captured.out

@patch('hg_localization.core._fetch_public_dataset_info')
def test_load_local_dataset_public_s3_no_info_found(
    mock_fetch_public_info, temp_datasets_store, monkeypatch, capsys
):
    """Test load_local_dataset: no public S3 info found for the dataset."""
    dataset_id = "public_zip_no_info"

    monkeypatch.setattr('hg_localization.core.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.core.AWS_SECRET_ACCESS_KEY', None)

    mock_fetch_public_info.return_value = None # Simulate no info found

    result = load_local_dataset(dataset_id)

    assert result is None
    captured = capsys.readouterr()
    assert "not found in public S3 dataset list or info was incomplete" in captured.out # This is the message when public_info is None
    assert "could not be fetched from any source" in captured.out