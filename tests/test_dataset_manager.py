from unittest.mock import MagicMock, patch, call, ANY
import os
import shutil
import tempfile
import json
import requests
from pathlib import Path
from datasets import Dataset, DatasetDict
from botocore.exceptions import ClientError
import pytest

# Functions/classes to test from dataset_manager.py
from hg_localization.dataset_manager import (
    _get_dataset_path,
    _fetch_public_datasets_json_via_url, _fetch_public_dataset_info, # Internal helpers for public datasets
    get_dataset_card_url, get_dataset_card_content, get_cached_dataset_card_content,
    download_dataset, load_local_dataset, upload_dataset,
    list_local_datasets, list_s3_datasets,
    sync_local_dataset_to_s3, sync_all_local_to_s3
)

# Import the new configuration system
from hg_localization.config import HGLocalizationConfig, default_config
# Mocks for hf_hub and datasets will be needed for many tests

# --- Fixtures ---

@pytest.fixture
def temp_datasets_store(tmp_path, monkeypatch):
    """Creates a temporary directory for datasets store and patches the config."""
    store_path = tmp_path / "test_datasets_store"
    store_path.mkdir()
    # Patch the default_config instance to use our temp path
    monkeypatch.setattr(default_config, 'datasets_store_path', store_path)
    return store_path

@pytest.fixture
def test_config_dm(tmp_path):
    """Create a test configuration for dataset manager tests."""
    store_path = tmp_path / "test_dm_datasets_store"
    store_path.mkdir()
    
    return HGLocalizationConfig(
        s3_bucket_name="test-dm-bucket",
        s3_endpoint_url="http://localhost:9000",
        aws_access_key_id="test-dm-access-key",
        aws_secret_access_key="test-dm-secret-key",
        s3_data_prefix="test/dm/prefix",
        datasets_store_path=store_path,
        default_config_name="test_dm_config",
        default_revision_name="test_dm_revision",
        public_datasets_json_key="test_dm_public_datasets.json",
        public_datasets_zip_dir_prefix="test_dm_public_datasets_zip"
    )

@pytest.fixture
def mock_hf_datasets_apis(mocker):
    """Mocks Hugging Face datasets library calls (load_dataset, save_to_disk, load_from_disk)."""
    mock_load_dataset = mocker.patch('hg_localization.dataset_manager.load_dataset', autospec=True)
    mock_dataset_instance = MagicMock(spec=DatasetDict) 
    mock_dataset_instance.save_to_disk = MagicMock()
    mock_load_dataset.return_value = mock_dataset_instance

    mock_load_from_disk = mocker.patch('hg_localization.dataset_manager.load_from_disk', autospec=True)
    mock_loaded_data_from_disk = MagicMock(spec=DatasetDict) 
    mock_load_from_disk.return_value = mock_loaded_data_from_disk
    
    return {
        "load_dataset": mock_load_dataset,
        "returned_dataset_instance": mock_dataset_instance,
        "load_from_disk": mock_load_from_disk,
        "data_loaded_from_disk": mock_loaded_data_from_disk
    }

@pytest.fixture
def mock_s3_utils_for_dm(mocker):
    """Mocks functions imported from s3_utils into dataset_manager."""
    mock_get_s3_cli = mocker.patch('hg_localization.dataset_manager._get_s3_client')
    mock_get_s3_prefix_dm = mocker.patch('hg_localization.dataset_manager._get_s3_prefix')
    mock_get_prefixed_s3_key_dm = mocker.patch('hg_localization.dataset_manager._get_prefixed_s3_key')
    mock_check_exists = mocker.patch('hg_localization.dataset_manager._check_s3_dataset_exists')
    mock_upload_dir = mocker.patch('hg_localization.dataset_manager._upload_directory_to_s3')
    mock_download_dir = mocker.patch('hg_localization.dataset_manager._download_directory_from_s3')
    mock_update_json = mocker.patch('hg_localization.dataset_manager._update_public_datasets_json')
    mock_get_public_url = mocker.patch('hg_localization.dataset_manager._get_s3_public_url')
    mock_get_presigned_card_url = mocker.patch('hg_localization.dataset_manager.get_s3_dataset_card_presigned_url')
    
    mock_s3_cli_instance = MagicMock()
    mock_get_s3_cli.return_value = mock_s3_cli_instance
    mock_get_s3_prefix_dm.return_value = "mocked/s3/prefix"
    def mock_get_prefixed_s3_key_side_effect(key, config=None):
        return f"global_prefix/{key}"
    mock_get_prefixed_s3_key_dm.side_effect = mock_get_prefixed_s3_key_side_effect
    def mock_get_public_url_side_effect(bucket, key, endpoint=None):
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    mock_get_public_url.side_effect = mock_get_public_url_side_effect

    return {
        "_get_s3_client": mock_get_s3_cli,
        "s3_client_instance": mock_s3_cli_instance,
        "_get_s3_prefix": mock_get_s3_prefix_dm,
        "_get_prefixed_s3_key": mock_get_prefixed_s3_key_dm,
        "_check_s3_dataset_exists": mock_check_exists,
        "_upload_directory_to_s3": mock_upload_dir,
        "_download_directory_from_s3": mock_download_dir,
        "_update_public_datasets_json": mock_update_json,
        "_get_s3_public_url": mock_get_public_url,
        "get_s3_dataset_card_presigned_url": mock_get_presigned_card_url
    }

@pytest.fixture
def mock_utils_for_dm(mocker):
    """Mocks functions imported from utils into dataset_manager."""
    mock_get_safe_path = mocker.patch('hg_localization.dataset_manager._get_safe_path_component')
    mock_zip_dir = mocker.patch('hg_localization.dataset_manager._zip_directory')
    mock_unzip_file = mocker.patch('hg_localization.dataset_manager._unzip_file')
    
    mock_get_safe_path.side_effect = lambda name: name.replace("/", "_").replace("\\", "_") if name else ""
    
    return {
        "_get_safe_path_component": mock_get_safe_path,
        "_zip_directory": mock_zip_dir,
        "_unzip_file": mock_unzip_file
    }

@pytest.fixture
def mock_aws_creds_for_dm(monkeypatch):
    """Patches AWS credentials in default_config for dataset_manager tests."""
    monkeypatch.setattr(default_config, 'aws_access_key_id', "test_access_key")
    monkeypatch.setattr(default_config, 'aws_secret_access_key', "test_secret_key")
    monkeypatch.setattr(default_config, 's3_bucket_name', "test-bucket")

# --- Tests for _get_dataset_path (specific to dataset_manager) ---
def test_dm_get_dataset_path(temp_datasets_store, mock_utils_for_dm):
    base_path = temp_datasets_store
    
    # Test private path (default)
    path1 = _get_dataset_path("test/ds1", config=default_config)
    assert path1 == base_path / "test_ds1" / default_config.default_config_name / default_config.default_revision_name
    
    # Test public path
    path2 = _get_dataset_path("test/ds1", config=default_config, is_public=True)
    assert path2 == base_path / "public" / "test_ds1" / default_config.default_config_name / default_config.default_revision_name
    
    mock_utils_for_dm["_get_safe_path_component"].assert_any_call("test/ds1")

# --- Tests for _fetch_public_datasets_json_via_url ---

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_no_bucket_name(mock_requests_get, capsys):
    config = HGLocalizationConfig(s3_bucket_name=None)
    result = _fetch_public_datasets_json_via_url(config=config)
    assert result is None
    captured = capsys.readouterr()
    assert "config.s3_bucket_name not configured" in captured.out
    mock_requests_get.assert_not_called()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_http_error_not_404(mock_requests_get, mock_s3_utils_for_dm, capsys):
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    expected_url = "https://test-bucket.s3.amazonaws.com/global_prefix/public_datasets.json"
    
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error", response=MagicMock(status_code=500))
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_datasets_json_via_url(config=config)
    assert result is None
    captured = capsys.readouterr()
    assert f"HTTP error fetching {expected_url}: 500 Server Error" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_http_error_404(mock_requests_get, mock_s3_utils_for_dm, capsys):
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    expected_url = "https://test-bucket.s3.amazonaws.com/global_prefix/public_datasets.json"

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error", response=MagicMock(status_code=404))
    mock_requests_get.return_value = mock_response

    result = _fetch_public_datasets_json_via_url(config=config)
    assert result is None
    captured = capsys.readouterr()
    assert f"global_prefix/{config.public_datasets_json_key} not found at the public URL" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_request_exception(mock_requests_get, mock_s3_utils_for_dm, capsys):
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    expected_url = "https://test-bucket.s3.amazonaws.com/global_prefix/public_datasets.json"
    
    mock_requests_get.side_effect = requests.exceptions.RequestException("Connection error")
    
    result = _fetch_public_datasets_json_via_url(config=config)
    assert result is None
    captured = capsys.readouterr()
    assert f"Error fetching {expected_url}: Connection error" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_json_decode_error(mock_requests_get, mock_s3_utils_for_dm, capsys):
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    expected_url = "https://test-bucket.s3.amazonaws.com/global_prefix/public_datasets.json"

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None 
    mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "doc", 0)
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_datasets_json_via_url(config=config)
    assert result is None
    captured = capsys.readouterr()
    assert f"Error: Content at {expected_url} is not valid JSON" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_success(mock_requests_get, mock_s3_utils_for_dm):
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    expected_url = "https://test-bucket.s3.amazonaws.com/global_prefix/public_datasets.json"

    expected_json = {"key": "value"}
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = expected_json
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_datasets_json_via_url(config=config)
    assert result == expected_json
    mock_requests_get.assert_called_once_with(expected_url, timeout=10)


# --- Tests for _fetch_public_dataset_info ---

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_fetch_public_dataset_info_fetch_json_fails(mock_fetch_json_url, capsys):
    mock_fetch_json_url.return_value = None
    result = _fetch_public_dataset_info("ds_id", "cfg", "rev")
    assert result is None
    mock_fetch_json_url.assert_called_once()
    # No specific print from _fetch_public_dataset_info itself if _fetch_public_datasets_json_via_url returns None and prints

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_fetch_public_dataset_info_not_found(mock_fetch_json_url, capsys):
    mock_fetch_json_url.return_value = {"another_key": "value"}
    dataset_id = "my_dataset"
    config_name = "config1"
    revision = "v1"
    result = _fetch_public_dataset_info(dataset_id, config_name, revision)
    assert result is None
    captured = capsys.readouterr()
    entry_key = f"{dataset_id}---{config_name}---{revision}"
    assert f"Public dataset info not found for {entry_key}" in captured.out

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_fetch_public_dataset_info_incomplete_data(mock_fetch_json_url, capsys):
    dataset_id = "my_dataset"
    config_name = "config1"
    revision = "v1"
    entry_key = f"{dataset_id}---{config_name}---{revision}"
    public_json_content = {
        entry_key: {"s3_zip_key": "some/key.zip"} # Missing s3_bucket
    }
    mock_fetch_json_url.return_value = public_json_content
    result = _fetch_public_dataset_info(dataset_id, config_name, revision)
    assert result is None
    captured = capsys.readouterr()
    assert f"Error: Public dataset info for {entry_key} is incomplete" in captured.out

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_fetch_public_dataset_info_success(mock_fetch_json_url):
    dataset_id = "my_dataset"
    config_name = "default"
    revision = None # Test with None revision
    entry_key = f"{dataset_id}---{config_name}---{default_config.default_revision_name}"
    expected_info = {"s3_zip_key": "some/key.zip", "s3_bucket": "bucket-name"}
    public_json_content = {
        entry_key: expected_info
    }
    mock_fetch_json_url.return_value = public_json_content
    result = _fetch_public_dataset_info(dataset_id, config_name, revision)
    assert result == expected_info

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_fetch_public_dataset_info_success_with_default_names(mock_fetch_json_url):
    dataset_id = "my_dataset_defaults"
    # Test with None config_name and revision, expecting default names to be used in key
    entry_key = f"{dataset_id}---{default_config.default_config_name}---{default_config.default_revision_name}"
    expected_info = {"s3_zip_key": "defaults/key.zip", "s3_bucket": "bucket-defaults"}
    public_json_content = {
        entry_key: expected_info
    }
    mock_fetch_json_url.return_value = public_json_content
    result = _fetch_public_dataset_info(dataset_id, None, None) # Pass None for config and revision
    assert result == expected_info

# --- Tests for Dataset Card Utilities ---

def test_get_dataset_card_url():
    dataset_id = "user/my_awesome_dataset"
    expected_url = "https://huggingface.co/datasets/user/my_awesome_dataset"
    assert get_dataset_card_url(dataset_id) == expected_url

@patch('hg_localization.dataset_manager.ModelCard.load')
def test_get_dataset_card_content_success(mock_model_card_load):
    mock_card = MagicMock()
    mock_card.text = "This is the card content."
    mock_model_card_load.return_value = mock_card
    dataset_id = "org/dataset"
    revision = "main"

    content = get_dataset_card_content(dataset_id, revision=revision)
    assert content == "This is the card content."
    mock_model_card_load.assert_called_once_with(dataset_id, repo_type="dataset", revision=revision)

@patch('hg_localization.dataset_manager.ModelCard.load')
def test_get_dataset_card_content_failure(mock_model_card_load, capsys):
    mock_model_card_load.side_effect = Exception("HF Hub down")
    dataset_id = "org/another_dataset"
    revision = "dev"

    content = get_dataset_card_content(dataset_id, revision=revision)
    assert content is None
    mock_model_card_load.assert_called_once_with(dataset_id, repo_type="dataset", revision=revision)
    captured = capsys.readouterr()
    assert "Error loading dataset card for 'org/another_dataset' (revision: dev): HF Hub down" in captured.out


def test_get_cached_dataset_card_content_local_exists(temp_datasets_store, mock_utils_for_dm, capsys):
    dataset_id = "local_card_ds"
    config_name = "cfg"
    revision = "rev"
    card_content = "Local card data!"

    # Simulate _get_dataset_path behavior for structuring the path
    # _get_safe_path_component is already mocked by mock_utils_for_dm
    ds_path = _get_dataset_path(dataset_id, config_name, revision, config=default_config) # Uses patched DATASETS_STORE_PATH
    os.makedirs(ds_path, exist_ok=True)
    local_card_file = ds_path / "dataset_card.md"
    with open(local_card_file, "w", encoding="utf-8") as f:
        f.write(card_content)

    retrieved_content = get_cached_dataset_card_content(dataset_id, config_name, revision, config=default_config)
    assert retrieved_content == card_content
    captured = capsys.readouterr()
    assert f"Found dataset card locally for (dataset: {dataset_id}, config: {config_name}, revision: {revision})" in captured.out

@patch("builtins.open", side_effect=IOError("Read permission denied"))
def test_get_cached_dataset_card_content_local_exists_read_error(mock_open, temp_datasets_store, mock_utils_for_dm, capsys):
    dataset_id = "local_card_io_error"
    ds_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(ds_path, exist_ok=True)
    (ds_path / "dataset_card.md").touch() # File exists

    # Ensure S3 utils are not called by making client None
    with patch('hg_localization.dataset_manager._get_s3_client') as mock_get_s3_cli:
        mock_get_s3_cli.return_value = None
        retrieved_content = get_cached_dataset_card_content(dataset_id, config=default_config)
        assert retrieved_content is None
        captured = capsys.readouterr()
        assert "Error reading local dataset card" in captured.out
        assert "Read permission denied" in captured.out
        mock_open.assert_called_once() # builtins.open was attempted

def test_get_cached_dataset_card_content_s3_success(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, capsys):
    dataset_id = "s3_card_ds"
    config_name = "s3_cfg"
    revision = "s3_rev"
    s3_card_content = "S3 card data!"
    
    # Create a test config with S3 bucket configured
    test_config = HGLocalizationConfig(
        s3_bucket_name="test-s3-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )

    # Ensure S3 client is returned
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # _get_s3_prefix is mocked by mock_s3_utils_for_dm to return "mocked/s3/prefix"
    # This prefix is used to construct the s3_card_key
    expected_s3_card_key = "mocked/s3/prefix/dataset_card.md" 

    ds_path = _get_dataset_path(dataset_id, config_name, revision, config=test_config)
    # local_card_file_path will be ds_path / "dataset_card.md"

    def mock_download_file(Bucket, Key, Filename):
        assert Bucket == test_config.s3_bucket_name
        assert Key == expected_s3_card_key
        with open(Filename, "w", encoding="utf-8") as f:
            f.write(s3_card_content)
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = mock_download_file

    retrieved_content = get_cached_dataset_card_content(dataset_id, config_name, revision, config=test_config)
    assert retrieved_content == s3_card_content
    mock_s3_utils_for_dm["s3_client_instance"].download_file.assert_called_once()
    assert (ds_path / "dataset_card.md").exists()
    captured = capsys.readouterr()
    assert f"Attempting to download dataset card from S3: s3://{test_config.s3_bucket_name}/{expected_s3_card_key}" in captured.out
    assert f"Successfully downloaded dataset card from S3 to {ds_path / 'dataset_card.md'}" in captured.out

def test_get_cached_dataset_card_content_s3_client_error_404(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, capsys):
    dataset_id = "s3_card_404"
    
    # Create a test config with S3 bucket configured
    test_config = HGLocalizationConfig(
        s3_bucket_name="test-s3-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )
    
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = ClientError(error_response, 'DownloadFile')
    
    retrieved_content = get_cached_dataset_card_content(dataset_id, config=test_config)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert "Dataset card not found on S3 at mocked/s3/prefix/dataset_card.md" in captured.out

def test_get_cached_dataset_card_content_s3_client_error_other(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, capsys):
    dataset_id = "s3_card_other_error"
    
    # Create a test config with S3 bucket configured
    test_config = HGLocalizationConfig(
        s3_bucket_name="test-s3-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )
    
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    error_response = {'Error': {'Code': '500', 'Message': 'Server Error'}}
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = ClientError(error_response, 'DownloadFile')

    retrieved_content = get_cached_dataset_card_content(dataset_id, config=test_config)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert "S3 ClientError when trying to download dataset card mocked/s3/prefix/dataset_card.md" in captured.out

@patch("builtins.open", side_effect=IOError("Post-download read error"))
def test_get_cached_dataset_card_content_s3_download_io_error(mock_open_after_dl, temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys):
    dataset_id = "s3_card_dl_io_error"
    
    # Create a test config with S3 bucket configured
    test_config = HGLocalizationConfig(
        s3_bucket_name="test-s3-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )
    
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]

    # Mock download_file to succeed (it creates the file marker)
    def fake_download(Bucket, Key, Filename):
        # Simulate the file being created by download_file, open will be called later to read it.
        Path(Filename).touch() 
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = fake_download

    retrieved_content = get_cached_dataset_card_content(dataset_id, config=test_config)
    assert retrieved_content is None
    captured = capsys.readouterr()
    # The first open is for local check (fails), then S3 download happens, then second open for reading downloaded file (fails)
    assert mock_open_after_dl.call_count >= 1 # Could be 1 if local check skipped or 2 if both attempted
    assert "IOError after downloading dataset card from S3" in captured.out
    assert "Post-download read error" in captured.out 

def test_get_cached_dataset_card_content_s3_not_configured(temp_datasets_store, mock_s3_utils_for_dm, capsys):
    dataset_id = "no_s3_card_ds"
    # S3 client returns None, or S3_BUCKET_NAME is None
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None
    # Or, alternatively: config = HGLocalizationConfig(s3_bucket_name=None)

    retrieved_content = get_cached_dataset_card_content(dataset_id, config=default_config)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert "S3 client not available or bucket not configured. Cannot fetch dataset card from S3." in captured.out

def test_get_cached_dataset_card_content_not_found_anywhere(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, capsys):
    dataset_id = "card_never_found"
    config_name = "cfg_never"
    revision = "rev_never"
    
    # Create a test config with S3 bucket configured
    test_config = HGLocalizationConfig(
        s3_bucket_name="test-s3-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )
    
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # S3 download_file results in 404
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = ClientError(error_response, 'DownloadFile')

    retrieved_content = get_cached_dataset_card_content(dataset_id, config_name, revision, config=test_config)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert f"Dataset card not found or readable locally for (dataset: {dataset_id}, config: {config_name}, revision: {revision})" in captured.out
    assert "Dataset card not found on S3" in captured.out


# --- Tests for download_dataset ---
# (These tests will be similar to the ones in the original test_core.py, 
#  but need to use the fixtures mocking dependencies of dataset_manager)

def test_download_dataset_new_full_spec(
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    dataset_id = "new_dataset"
    config_name = "config1"
    revision = "v1.0"

    # Simulate S3 not configured or dataset not found on S3
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None # No S3 client
    # OR: mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = False
    
    # Mock get_dataset_card_content call within download_dataset
    with patch('hg_localization.dataset_manager.get_dataset_card_content') as mock_get_card:
        mock_get_card.return_value = "Mocked card data"

        success, msg_path_str = download_dataset(
            dataset_id, config_name=config_name, revision=revision, 
            trust_remote_code=True, make_public=False, skip_s3_upload=True # Skip S3 for this basic test
        , config=default_config)

        assert success is True
        expected_path = temp_datasets_store / "new_dataset" / "config1" / "v1.0"
        assert Path(msg_path_str) == expected_path
        mock_hf_datasets_apis["load_dataset"].assert_called_once_with(
            path=dataset_id, name=config_name, revision=revision, trust_remote_code=True
        )
        mock_hf_datasets_apis["returned_dataset_instance"].save_to_disk.assert_called_once_with(str(expected_path))
        mock_get_card.assert_called_once_with(dataset_id, revision=revision)
        assert (expected_path / "dataset_card.md").exists()
        assert (expected_path / "dataset_card.md").read_text() == "Mocked card data"
        captured = capsys.readouterr()
        assert f"Dataset '{dataset_id}' (config: {config_name}, revision: {revision}) successfully saved" in captured.out
        assert "Skipping S3 upload" in captured.out # Due to skip_s3_upload=True

# Example: Adapting a test for S3 functionality in download_dataset
@patch('hg_localization.dataset_manager.get_dataset_card_content') # Mock card fetching within download
def test_download_dataset_from_s3_success(
    mock_get_card, temp_datasets_store, mock_hf_datasets_apis, 
    mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys
):
    dataset_id = "s3_ds_download"
    config_name = "s3_cfg"
    
    # Create a test config with S3 bucket configured
    test_config = HGLocalizationConfig(
        s3_bucket_name="my-s3-bucket-for-dm",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )

    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    
    local_save_path = _get_dataset_path(dataset_id, config_name, config=test_config)
    
    def side_effect_s3_download(*args, **kwargs):
        dl_local_path = args[1] 
        os.makedirs(dl_local_path, exist_ok=True)
        (dl_local_path / "dataset_info.json").touch()
        return True
    mock_s3_utils_for_dm["_download_directory_from_s3"].side_effect = side_effect_s3_download
    mock_get_card.return_value = None

    success, msg_path = download_dataset(dataset_id, config_name=config_name, config=test_config)

    assert success is True
    assert Path(msg_path) == local_save_path
    mock_s3_utils_for_dm["_get_s3_client"].assert_called() 
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].assert_called_once() 
    mock_s3_utils_for_dm["_download_directory_from_s3"].assert_called_once_with(
        mock_s3_utils_for_dm["_get_s3_client"].return_value, local_save_path, test_config.s3_bucket_name, "mocked/s3/prefix"
    )
    mock_hf_datasets_apis["load_dataset"].assert_not_called()
    captured = capsys.readouterr()
    assert f"Dataset found on S3. Attempting to download from S3 to local cache: {local_save_path}" in captured.out
    assert f"Successfully downloaded dataset from S3 to {local_save_path}" in captured.out


# --- Tests for load_local_dataset ---
def test_load_local_dataset_success_local_exists(
    temp_datasets_store, mock_hf_datasets_apis, mock_utils_for_dm, capsys
):
    dataset_id = "local_loader_test"
    config_name = "cfg_load"
    dataset_path = _get_dataset_path(dataset_id, config_name=config_name, config=default_config) 
    os.makedirs(dataset_path, exist_ok=True)
    (dataset_path / "dataset_info.json").touch()

    expected_data_obj = mock_hf_datasets_apis["data_loaded_from_disk"]
    loaded_data = load_local_dataset(dataset_id, config_name=config_name, config=default_config)

    assert loaded_data == expected_data_obj
    mock_hf_datasets_apis["load_from_disk"].assert_called_once_with(str(dataset_path))
    captured = capsys.readouterr()
    assert f"Loading dataset '{dataset_id}' (config: {config_name}, revision: default revision) from {dataset_path}" in captured.out

def test_load_local_dataset_cache_miss_auth_s3_download_success(
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    dataset_id = "auth_s3_ds"
    config_name = "main_config"
    revision = "v1.1"
    
    # Create a test config with S3 bucket configured
    test_config = HGLocalizationConfig(
        s3_bucket_name="my-auth-s3-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )

    # Ensure local paths do NOT exist initially (both public and private)
    private_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=test_config, is_public=False)
    public_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=test_config, is_public=True)
    assert not private_dataset_path.exists()
    assert not public_dataset_path.exists()

    # Configure S3 credentials and bucket
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_get_s3_prefix"].return_value = f"s3_prefix_for_{dataset_id}"

    def mock_s3_download_success_effect(s3_client, local_path_to_save, bucket, s3_prefix):
        # Simulate successful download by creating the directory and a marker file
        os.makedirs(local_path_to_save, exist_ok=True)
        (local_path_to_save / "dataset_info.json").touch()
        return True
    mock_s3_utils_for_dm["_download_directory_from_s3"].side_effect = mock_s3_download_success_effect

    expected_data_obj = mock_hf_datasets_apis["data_loaded_from_disk"]
    loaded_data = load_local_dataset(dataset_id, config_name, revision, config=test_config)

    assert loaded_data == expected_data_obj
    mock_s3_utils_for_dm["_get_s3_client"].assert_called_once()
    # The download should go to private path since this is authenticated S3 download
    mock_s3_utils_for_dm["_download_directory_from_s3"].assert_called_once_with(
        mock_s3_utils_for_dm["s3_client_instance"],
        private_dataset_path,
        test_config.s3_bucket_name,
        f"s3_prefix_for_{dataset_id}"
    )
    mock_hf_datasets_apis["load_from_disk"].assert_called_once_with(str(private_dataset_path))
    
    captured = capsys.readouterr()
    version_str = f"(config: {config_name or test_config.default_config_name.replace('_',' ')}, revision: {revision or test_config.default_revision_name.replace('_',' ')})"
    assert f"Dataset '{dataset_id}' {version_str} not found in local cache" in captured.out
    assert "Attempting to fetch from S3 using credentials..." in captured.out
    assert f"Successfully downloaded from S3 (authenticated) to {private_dataset_path}" in captured.out
    assert f"Loading dataset '{dataset_id}' {version_str} from {private_dataset_path}" in captured.out

def test_load_local_dataset_cache_miss_auth_s3_fails_public_s3_fails(
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    dataset_id = "auth_s3_fail_public_fail_ds"
    config_name = "cfg_x"
    revision = "rev_y"
    s3_bucket_val = "my-auth-s3-fail-bucket"

    # Ensure local paths do NOT exist initially (both public and private)
    private_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=default_config, is_public=False)
    public_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=default_config, is_public=True)
    assert not private_dataset_path.exists()
    assert not public_dataset_path.exists()

    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_get_s3_prefix"].return_value = f"s3_prefix_for_{dataset_id}_auth_fail"
    mock_s3_utils_for_dm["_download_directory_from_s3"].return_value = False # Auth S3 download fails

    # Mock _fetch_public_dataset_info to also return None (public S3 path fails)
    with patch("hg_localization.dataset_manager._fetch_public_dataset_info") as mock_fetch_public_info:
        mock_fetch_public_info.return_value = None

        loaded_data = load_local_dataset(dataset_id, config_name, revision, config=default_config)

        assert loaded_data is None
        mock_s3_utils_for_dm["_download_directory_from_s3"].assert_called_once()
        mock_fetch_public_info.assert_called_once_with(dataset_id, config_name, revision, default_config)
        mock_hf_datasets_apis["load_from_disk"].assert_not_called()

        captured = capsys.readouterr()
        version_str = f"(config: {config_name or default_config.default_config_name.replace('_',' ')}, revision: {revision or default_config.default_revision_name.replace('_',' ')})"
        assert f"Failed to download '{dataset_id}' {version_str} from S3 (authenticated) or not found." in captured.out
        assert "Attempting to fetch from public S3 dataset list via URL..." in captured.out
        # This next assertion depends on the exact wording when _fetch_public_dataset_info returns None and dataset is still not found
        assert f"Dataset '{dataset_id}' {version_str} not found in public S3 dataset list or info was incomplete." in captured.out
        assert f"Dataset '{dataset_id}' {version_str} could not be fetched from any source." in captured.out

@patch("requests.get")
@patch("hg_localization.dataset_manager._fetch_public_dataset_info")
def test_load_local_dataset_cache_miss_no_auth_public_s3_success(
    mock_fetch_public_info, mock_requests_get,
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    dataset_id = "public_s3_ds"
    config_name = "default_cfg"
    revision = "latest"
    public_s3_bucket = "my-public-bucket"
    public_zip_key = "zips/public_s3_ds.zip"
    # full_public_zip_s3_key will be formed by _get_prefixed_s3_key mock
    # public_zip_url will be formed by _get_s3_public_url mock

    # Create a config without AWS credentials to simulate no auth scenario
    no_auth_config = HGLocalizationConfig(
        s3_bucket_name="zheyu-huggingface-test",
        aws_access_key_id=None,
        aws_secret_access_key=None,
        s3_data_prefix="global_prefix",
        public_datasets_json_key="public_datasets.json",
        public_datasets_zip_dir_prefix="public_datasets_zip",
        datasets_store_path=temp_datasets_store  # Use the temp path from fixture
    )
    
    # For public downloads, the dataset should be saved to public path
    public_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=no_auth_config, is_public=True)
    private_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=no_auth_config, is_public=False)
    assert not public_dataset_path.exists()
    assert not private_dataset_path.exists()

    # Mock _fetch_public_dataset_info to return success
    mock_fetch_public_info.return_value = {
        "s3_zip_key": public_zip_key, # This is relative to default_config.s3_data_prefix or root
        "s3_bucket": public_s3_bucket
    }
    
    # The mock_s3_utils_for_dm fixture defines a side_effect for _get_prefixed_s3_key:
    # lambda key: f"global_prefix/{key}"
    # So, when load_local_dataset calls _get_prefixed_s3_key(public_zip_key),
    # it will receive f"global_prefix/{public_zip_key}".
    # Our assertion for _get_s3_public_url must expect this key.
    actual_key_that_will_be_passed_to_get_s3_public_url = f"global_prefix/{public_zip_key.lstrip('/')}"

    # Get the S3_ENDPOINT_URL that load_local_dataset will see (it's patched to None for this test)
    current_s3_endpoint_url = None 

    # The URL that _get_s3_public_url mock should return to the code under test
    # This URL should be constructed with the key that _get_s3_public_url will actually receive.
    expected_public_zip_url = f"https://{no_auth_config.s3_bucket_name}.s3.amazonaws.com/{actual_key_that_will_be_passed_to_get_s3_public_url}"
    mock_s3_utils_for_dm["_get_s3_public_url"].return_value = expected_public_zip_url

    # We don't need to modify mock_s3_utils_for_dm["_get_prefixed_s3_key"] here;
    # its side_effect from the fixture is what determines the actual behavior.

    # Mock requests.get for downloading the zip
    mock_zip_content = b"zip_file_content_bytes"
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = iter([mock_zip_content])
    mock_requests_get.return_value = mock_response

    # Mock _unzip_file to simulate successful unzipping and dataset file creation
    def mock_unzip_success_effect(zip_file_path, target_dir):
        os.makedirs(target_dir, exist_ok=True)
        (target_dir / "dataset_info.json").touch() # Or dataset_dict.json
        return True
    mock_utils_for_dm["_unzip_file"].side_effect = mock_unzip_success_effect

    expected_data_obj = mock_hf_datasets_apis["data_loaded_from_disk"]
    loaded_data = load_local_dataset(dataset_id, config_name, revision, config=no_auth_config)

    assert loaded_data == expected_data_obj
    # Assertions for S3 client / S3 download should ensure they are NOT called if no auth creds
    # Check if _get_s3_client was called within the non-authenticated S3 part (it shouldn't be for download)
    # load_local_dataset calls _get_s3_client for auth download if creds exist. If not, it skips.
    # So, if AWS_ACCESS_KEY_ID is None, _get_s3_client in dataset_manager won't be called for the auth path.
    # The mock_s3_utils_for_dm["_get_s3_client"] is for s3_utils functions, not directly for dm._get_s3_client call. 
    # Need to be careful about what is being asserted here.
    # The important thing is _download_directory_from_s3 is not called.
    mock_s3_utils_for_dm["_download_directory_from_s3"].assert_not_called()

    mock_fetch_public_info.assert_called_once_with(dataset_id, config_name, revision, no_auth_config)
    # The key passed to _get_s3_public_url is the *full* key after _get_prefixed_s3_key mock (from fixture) runs.
    # The S3_ENDPOINT_URL passed to _get_s3_public_url within load_local_dataset is the one from hg_localization.dataset_manager.S3_ENDPOINT_URL
    mock_s3_utils_for_dm["_get_s3_public_url"].assert_called_once_with(
        no_auth_config.s3_bucket_name,  # The code uses the configured bucket name, not the one from public info
        actual_key_that_will_be_passed_to_get_s3_public_url, 
        current_s3_endpoint_url
    )
    mock_requests_get.assert_called_once_with(expected_public_zip_url, stream=True, timeout=300)
    mock_utils_for_dm["_unzip_file"].assert_called_once_with(ANY, public_dataset_path) # ANY for temp zip path, public path for target
    mock_hf_datasets_apis["load_from_disk"].assert_called_once_with(str(public_dataset_path))

    captured = capsys.readouterr()
    assert "Attempting to fetch from S3 using credentials..." not in captured.out # Ensure auth path was skipped
    assert "Attempting to fetch from public S3 dataset list via URL..." in captured.out
    assert f"Public dataset zip found. Attempting download from: {expected_public_zip_url}" in captured.out
    assert "Public zip downloaded to" in captured.out
    assert f"Successfully downloaded and unzipped public dataset to {public_dataset_path}" in captured.out


# --- Tests for upload_dataset ---

@pytest.fixture
def mock_dataset_obj():
    """Provides a mock Dataset or DatasetDict object for upload_dataset tests."""
    mock_ds = MagicMock(spec=DatasetDict) # Or Dataset, depending on typical usage
    mock_ds.save_to_disk = MagicMock()
    return mock_ds

def test_upload_dataset_local_save_failure(
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, mock_utils_for_dm, capsys
):
    dataset_id = "upload_fail_local"
    mock_dataset_obj.save_to_disk.side_effect = Exception("Disk full!")

    success = upload_dataset(mock_dataset_obj, dataset_id, config=default_config)

    assert success is False
    mock_dataset_obj.save_to_disk.assert_called_once()
    mock_s3_utils_for_dm["_get_s3_client"].assert_not_called() # S3 part should not be reached
    captured = capsys.readouterr()
    assert f"Error saving dataset '{dataset_id}'" in captured.out
    assert "Disk full!" in captured.out

def test_upload_dataset_s3_not_configured(
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    dataset_id = "upload_no_s3_config"
    # Simulate S3 not configured
    config = HGLocalizationConfig(s3_bucket_name=None)
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None # Or s3_client is None

    success = upload_dataset(mock_dataset_obj, dataset_id, make_public=True, config=default_config) # make_public should be skipped

    assert success is True # True because local save succeeded
    mock_dataset_obj.save_to_disk.assert_called_once()
    # _get_s3_client might be called to check, but _upload_directory_to_s3 shouldn't
    mock_s3_utils_for_dm["_upload_directory_to_s3"].assert_not_called()
    mock_utils_for_dm["_zip_directory"].assert_not_called() # make_public part skipped
    
    captured = capsys.readouterr()
    expected_local_path = _get_dataset_path(dataset_id, config=default_config) # Get expected path
    assert f"Dataset '{dataset_id}' (config: default, revision: default) successfully saved to local cache: {expected_local_path}" in captured.out
    assert "S3 not configured or client init failed; skipping S3 upload." in captured.out
    assert "Cannot make dataset public as S3 is not configured." in captured.out # Because make_public was True

def test_upload_dataset_s3_upload_success_no_make_public(
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    dataset_id = "upload_s3_success"
    config_name = "cfg_up"
    s3_bucket = "my-upload-bucket"
    
    # Create test config with the expected bucket name
    test_config = HGLocalizationConfig(
        s3_bucket_name=s3_bucket,
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )
    
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_upload_directory_to_s3"].return_value = True # Simulate success

    success = upload_dataset(mock_dataset_obj, dataset_id, config_name=config_name, make_public=False, config=test_config)

    assert success is True
    local_save_path = _get_dataset_path(dataset_id, config_name, config=test_config)
    mock_dataset_obj.save_to_disk.assert_called_once_with(str(local_save_path))
    mock_s3_utils_for_dm["_get_s3_client"].assert_called()
    mock_s3_utils_for_dm["_get_s3_prefix"].assert_called_with(dataset_id, config_name, ANY, test_config)
    mock_s3_utils_for_dm["_upload_directory_to_s3"].assert_called_once_with(
        mock_s3_utils_for_dm["s3_client_instance"],
        local_save_path,
        s3_bucket,
        mock_s3_utils_for_dm["_get_s3_prefix"].return_value
    )
    mock_utils_for_dm["_zip_directory"].assert_not_called() # make_public is False
    
    captured = capsys.readouterr()
    assert f"Successfully initiated upload of dataset '{dataset_id}'" in captured.out
    assert "Preparing to make (uploaded) dataset" not in captured.out

def test_upload_dataset_s3_upload_failure_no_make_public(
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    dataset_id = "upload_s3_fail"
    s3_bucket = "my-upload-fail-bucket"
    
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_upload_directory_to_s3"].side_effect = Exception("S3 Connection Error")

    success = upload_dataset(mock_dataset_obj, dataset_id, make_public=False, config=default_config)

    assert success is False # False because S3 upload failed (after local save succeeded)
    mock_dataset_obj.save_to_disk.assert_called_once()
    mock_s3_utils_for_dm["_upload_directory_to_s3"].assert_called_once()
    captured = capsys.readouterr()
    assert f"Error uploading dataset '{dataset_id}'" in captured.out
    assert "S3 Connection Error" in captured.out

@patch('tempfile.NamedTemporaryFile')
@patch('shutil.copytree')
def test_upload_dataset_make_public_success(
    mock_shutil_copytree, mock_tempfile_named,
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    dataset_id = "upload_public_success"
    config_name = "public_cfg"
    revision = "v_pub"
    s3_bucket = "my-public-upload-bucket"
    
    # Create test config with the expected bucket name
    test_config = HGLocalizationConfig(
        s3_bucket_name=s3_bucket,
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        datasets_store_path=temp_datasets_store
    )

    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_upload_directory_to_s3"].return_value = True # Private upload success
    mock_utils_for_dm["_zip_directory"].return_value = True # Zipping success
    mock_s3_utils_for_dm["_update_public_datasets_json"].return_value = True # Manifest update success

    # Mock NamedTemporaryFile to behave as expected
    mock_tmp_file_obj = MagicMock()
    mock_tmp_file_obj.name = str(temp_datasets_store / "temp_dataset.zip") # Needs to be a Path-like str
    mock_tempfile_named.return_value.__enter__.return_value = mock_tmp_file_obj


    success = upload_dataset(mock_dataset_obj, dataset_id, config_name=config_name, revision=revision, make_public=True, config=test_config)

    assert success is True
    local_save_path = _get_dataset_path(dataset_id, config_name, revision, config=test_config)
    mock_dataset_obj.save_to_disk.assert_called_once_with(str(local_save_path))
    mock_s3_utils_for_dm["_upload_directory_to_s3"].assert_called_once() # Private upload
    
    mock_shutil_copytree.assert_called_once() # copy into temp dir for zipping
    mock_utils_for_dm["_zip_directory"].assert_called_once() # Zipping for public
    
    # s3_client.upload_file for public zip
    # _get_safe_path_component will be called for dataset_id, config_name, revision
    # Construct expected zip file name and S3 key based on these safe names
    safe_ds_id = mock_utils_for_dm["_get_safe_path_component"](dataset_id)
    safe_cfg = mock_utils_for_dm["_get_safe_path_component"](config_name)
    safe_rev = mock_utils_for_dm["_get_safe_path_component"](revision)
    
    expected_zip_filename = f"{safe_ds_id}---{safe_cfg}---{safe_rev}.zip"
    expected_base_s3_zip_key = f"{test_config.public_datasets_zip_dir_prefix}/{expected_zip_filename}"
    # mock_s3_utils_for_dm["_get_prefixed_s3_key"] side_effect: lambda key: f"global_prefix/{key}"
    expected_s3_zip_key_full = f"global_prefix/{expected_base_s3_zip_key}"

    mock_s3_utils_for_dm["s3_client_instance"].upload_file.assert_called_once_with(
        mock_tmp_file_obj.name,
        s3_bucket,
        expected_s3_zip_key_full,
        ExtraArgs={'ACL': 'public-read'}
    )
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_called_once_with(
        mock_s3_utils_for_dm["s3_client_instance"],
        s3_bucket,
        dataset_id, config_name, revision, expected_base_s3_zip_key, test_config
    )

    captured = capsys.readouterr()
    assert "Successfully uploaded public zip" in captured.out
    assert "Successfully initiated upload of dataset" in captured.out # For private part
    assert f"Preparing to make (uploaded) dataset {dataset_id}" in captured.out

@patch('tempfile.NamedTemporaryFile')
@patch('shutil.copytree')
def test_upload_dataset_make_public_zip_failure(
    mock_shutil_copytree, mock_tempfile_named,
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    dataset_id = "upload_public_zip_fail"
    s3_bucket = "my-public-zip-fail-bucket"
    # Updated to use config object instead of monkeypatch
    
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_upload_directory_to_s3"].return_value = True # Private upload success
    mock_utils_for_dm["_zip_directory"].return_value = False # Zipping fails

    mock_tmp_file_obj = MagicMock()
    mock_tmp_file_obj.name = str(temp_datasets_store / "temp_dataset_zip_fail.zip")
    mock_tempfile_named.return_value.__enter__.return_value = mock_tmp_file_obj

    success = upload_dataset(mock_dataset_obj, dataset_id, make_public=True, config=default_config)
    
    assert success is True # Still true as private upload (if configured and successful) or local save succeeded
    mock_dataset_obj.save_to_disk.assert_called_once()
    mock_utils_for_dm["_zip_directory"].assert_called_once()
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.assert_not_called() # Public S3 upload_file
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_not_called()
    captured = capsys.readouterr()
    assert "Failed to zip dataset for public upload." in captured.out


@patch('tempfile.NamedTemporaryFile')
@patch('shutil.copytree')
def test_upload_dataset_make_public_s3_public_upload_failure(
    mock_shutil_copytree, mock_tempfile_named,
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    dataset_id = "upload_public_s3_fail"
    s3_bucket = "my-public_s3_fail-bucket"
    # Updated to use config object instead of monkeypatch

    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_upload_directory_to_s3"].return_value = True
    mock_utils_for_dm["_zip_directory"].return_value = True # Zipping succeeds
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.side_effect = Exception("Public S3 Upload Error")

    mock_tmp_file_obj = MagicMock()
    mock_tmp_file_obj.name = str(temp_datasets_store / "temp_dataset_public_s3_fail.zip")
    mock_tempfile_named.return_value.__enter__.return_value = mock_tmp_file_obj

    success = upload_dataset(mock_dataset_obj, dataset_id, make_public=True, config=default_config)

    assert success is True # Still true
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.assert_called_once() # Attempted public upload
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_not_called()
    captured = capsys.readouterr()
    assert "Failed to upload public zip" in captured.out
    assert "Public S3 Upload Error" in captured.out

@patch('tempfile.NamedTemporaryFile')
@patch('shutil.copytree')
def test_upload_dataset_make_public_update_json_failure(
    mock_shutil_copytree, mock_tempfile_named,
    temp_datasets_store, mock_dataset_obj, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    dataset_id = "upload_public_json_fail"
    s3_bucket = "my-public-json-fail-bucket"
    # Updated to use config object instead of monkeypatch

    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_upload_directory_to_s3"].return_value = True
    mock_utils_for_dm["_zip_directory"].return_value = True
    # s3_client.upload_file (for public zip) is implicitly successful (not mocked to fail)
    mock_s3_utils_for_dm["_update_public_datasets_json"].return_value = False # JSON update fails

    mock_tmp_file_obj = MagicMock()
    mock_tmp_file_obj.name = str(temp_datasets_store / "temp_dataset_json_fail.zip")
    mock_tempfile_named.return_value.__enter__.return_value = mock_tmp_file_obj

    success = upload_dataset(mock_dataset_obj, dataset_id, make_public=True, config=default_config)

    assert success is True # Still true
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.assert_called_once() # Public upload happened
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_called_once()
    captured = capsys.readouterr()
    # Message for failed JSON update is not explicitly checked here, but the function call is.
    # The function _update_public_datasets_json itself would print the error.
    # We primarily care that the call was made and it was reported as failed by the mock.
    # A more specific check could be to ensure a warning print from upload_dataset if it had one for this case.
    # Looking at upload_dataset, it does print a failure message from _update_public_datasets_json itself.

# --- Tests for list_local_datasets (similar to original test_core.py) ---

def test_list_local_datasets_empty_store(temp_datasets_store, capsys):
    """Test list_local_datasets with an empty DATASETS_STORE_PATH."""
    datasets = list_local_datasets(config=default_config)
    assert datasets == []
    captured = capsys.readouterr()
    assert "No local datasets found" in captured.out

def test_list_local_datasets_store_does_not_exist(temp_datasets_store, monkeypatch, capsys):
    """Test list_local_datasets when DATASETS_STORE_PATH does not exist."""
    # Simulate store path not existing by pointing to a non-existent subdir
    non_existent_store = temp_datasets_store / "non_existent"
    # Create a config with the non-existent store path
    config = HGLocalizationConfig(datasets_store_path=non_existent_store)
    
    datasets = list_local_datasets(config=config)
    assert datasets == []
    captured = capsys.readouterr()
    assert f"Local dataset store directory does not exist: {non_existent_store}" in captured.out

def test_list_local_datasets_with_non_dataset_files_and_dirs(temp_datasets_store, capsys):
    """Test list_local_datasets when the store contains files or empty dirs at the root."""
    (temp_datasets_store / "some_file.txt").touch()
    (temp_datasets_store / "empty_dir").mkdir()
    
    # Create a valid dataset structure to ensure it's still found
    ds_path = temp_datasets_store / "my_dataset_id" / "default" / "default_revision"
    os.makedirs(ds_path, exist_ok=True)
    (ds_path / "dataset_info.json").touch() # Marker file

    datasets = list_local_datasets(config=default_config)
    assert len(datasets) == 1
    assert datasets[0]["dataset_id"] == "my_dataset/id"
    
    captured = capsys.readouterr()
    # Ensure no errors about the extra file/dir, just the found dataset
    assert "Found 1 local dataset(s):" in captured.out
    # Check that there's no unexpected error output
    assert "error" not in captured.err.lower()

def test_list_local_datasets_various_structures(temp_datasets_store, mock_utils_for_dm, capsys):
    """Test list_local_datasets with various valid dataset structures."""
    # Dataset 1: default config and revision
    ds1_id_original = "user/dataset1"
    ds1_id_safe = mock_utils_for_dm["_get_safe_path_component"](ds1_id_original)
    ds1_path = temp_datasets_store / ds1_id_safe / default_config.default_config_name / default_config.default_revision_name
    os.makedirs(ds1_path, exist_ok=True)
    (ds1_path / "dataset_info.json").touch()
    (ds1_path / "dataset_card.md").write_text("Card for DS1")

    # Dataset 2: custom config, default revision
    ds2_id_original = "dataset2"
    ds2_id_safe = mock_utils_for_dm["_get_safe_path_component"](ds2_id_original)
    ds2_config = "custom_cfg"
    ds2_config_safe = mock_utils_for_dm["_get_safe_path_component"](ds2_config)
    ds2_path = temp_datasets_store / ds2_id_safe / ds2_config_safe / default_config.default_revision_name
    os.makedirs(ds2_path, exist_ok=True)
    (ds2_path / "dataset_info.json").touch()

    # Dataset 3: custom config and revision
    ds3_id_original = "another/dataset3"
    ds3_id_safe = mock_utils_for_dm["_get_safe_path_component"](ds3_id_original)
    ds3_config = "cfg_v2"
    ds3_config_safe = mock_utils_for_dm["_get_safe_path_component"](ds3_config)
    ds3_revision = "rev_2.0"
    ds3_revision_safe = mock_utils_for_dm["_get_safe_path_component"](ds3_revision)
    ds3_path = temp_datasets_store / ds3_id_safe / ds3_config_safe / ds3_revision_safe
    os.makedirs(ds3_path, exist_ok=True)
    (ds3_path / "dataset_info.json").touch()
    (ds3_path / "dataset_card.md").write_text("Card for DS3")


    # Create a dir that looks like a dataset_id but has no config/revision structure
    (temp_datasets_store / "not_a_full_dataset").mkdir()
    
    # Create a dir that looks like dataset_id/config but no revision structure
    (temp_datasets_store / "partial_dataset" / "config_only").mkdir(parents=True)


    datasets = list_local_datasets(config=default_config)
    assert len(datasets) == 3
    
    # Sort by dataset_id for consistent checking
    datasets_sorted = sorted(datasets, key=lambda d: d["dataset_id"])

    assert datasets_sorted[0]["dataset_id"] == ds3_id_original # another/dataset3 (restored from another_dataset3)
    assert datasets_sorted[0]["config_name"] == ds3_config
    assert datasets_sorted[0]["revision"] == ds3_revision
    assert datasets_sorted[0]["path"] == str(ds3_path)
    assert datasets_sorted[0]["has_card"] is True

    assert datasets_sorted[1]["dataset_id"] == ds2_id_original # dataset2 (no change, no underscore)
    assert datasets_sorted[1]["config_name"] == ds2_config
    assert datasets_sorted[1]["revision"] == None # Was default_config.default_revision_name, function returns None
    assert datasets_sorted[1]["path"] == str(ds2_path)
    assert datasets_sorted[1]["has_card"] is False

    assert datasets_sorted[2]["dataset_id"] == ds1_id_original # user/dataset1 (restored from user_dataset1)
    assert datasets_sorted[2]["config_name"] == None # Was default_config.default_config_name, function returns None
    assert datasets_sorted[2]["revision"] == None # Was default_config.default_revision_name, function returns None
    assert datasets_sorted[2]["path"] == str(ds1_path)
    assert datasets_sorted[2]["has_card"] is True
    
    captured = capsys.readouterr()
    assert f"Dataset ID: {ds1_id_original}, Config: None, Revision: None" in captured.out
    assert f"Path: {ds1_path}" in captured.out
    assert "Card: Yes" in captured.out
    assert f"Dataset ID: {ds2_id_original}, Config: {ds2_config}, Revision: None" in captured.out
    assert "Card: No" in captured.out
    assert "Found 3 local dataset(s):" in captured.out
    # Ensure the non-dataset dirs are not listed as errors, just ignored.
    assert "not_a_full_dataset" not in captured.out # or at least not as a failed dataset
    assert "partial_dataset" not in captured.out

def test_list_local_datasets_invalid_subdirs(temp_datasets_store, mock_utils_for_dm, capsys):
    """Test that directories not matching the expected structure are ignored."""
    # Dataset 1: valid
    ds1_id = "valid_ds"
    ds1_path = temp_datasets_store / ds1_id / "default" / "default_rev"
    os.makedirs(ds1_path, exist_ok=True)
    (ds1_path / "dataset_info.json").touch()

    # Invalid structure: dataset_id / file.txt (not a config dir)
    (temp_datasets_store / ds1_id / "some_file.txt").touch()
    
    # Invalid structure: dataset_id / config_dir / file.txt (not a revision dir)
    config_dir_for_file = temp_datasets_store / ds1_id / "config_with_file"
    os.makedirs(config_dir_for_file, exist_ok=True)
    (config_dir_for_file / "another_file.md").touch()

    # Invalid structure: dataset_id / config_dir / revision_dir / but_is_file (revision is a file)
    config_dir_for_rev_file = temp_datasets_store / ds1_id / "config_with_rev_file"
    os.makedirs(config_dir_for_rev_file, exist_ok=True)
    (config_dir_for_rev_file / "revision_as_file.json").touch()


    datasets = list_local_datasets(config=default_config)
    assert len(datasets) == 1
    assert datasets[0]["dataset_id"] == "valid/ds"
    
    captured = capsys.readouterr()
    # The function does print "Found X local dataset(s):"
    assert "Found 1 local dataset(s):" in captured.out
    # Check that there's no unexpected error output
    assert "error" not in captured.err.lower()

# --- Tests for list_s3_datasets ---

def test_list_s3_datasets_not_configured(mock_s3_utils_for_dm, capsys):
    """Test list_s3_datasets when S3 is not configured."""
    config = HGLocalizationConfig(s3_bucket_name=None)
    datasets = list_s3_datasets(config=config)
    assert datasets == []
    captured = capsys.readouterr()
    assert "config.s3_bucket_name not configured. Cannot list S3 datasets." in captured.out

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_list_s3_datasets_empty_bucket(mock_fetch_public_json, mock_s3_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm):
    """Test list_s3_datasets with an empty S3 bucket (no dataset prefixes)."""
    s3_bucket = "empty-s3-bucket"
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # s3_client_instance.get_paginator('list_objects_v2') will return a mock paginator
    # That mock paginator's paginate method should be configured.
    mock_paginator = mock_s3_utils_for_dm["s3_client_instance"].get_paginator.return_value
    mock_paginator.paginate.return_value = iter([]) # Simulate no pages / no results
    
    # Prevent fallback to public URL listing for this test
    mock_fetch_public_json.return_value = None 
    
    datasets = list_s3_datasets(config=default_config)
    assert datasets == []
    captured = capsys.readouterr()
    # Check for the message indicating no datasets found after scanning and failing/skipping public list
    assert f"No datasets found in S3 bucket '{default_config.s3_bucket_name}' by scanning or from public list." in captured.out
    
    # The actual prefix used by list_s3_datasets for the initial scan.
    # default_config.s3_data_prefix defaults to ""
    expected_scan_prefix = default_config.s3_data_prefix.strip('/') + '/' if default_config.s3_data_prefix else ""

    # We assert that paginate was called on the mock_paginator
    mock_s3_utils_for_dm["s3_client_instance"].get_paginator.assert_called_once_with('list_objects_v2')
    mock_paginator.paginate.assert_called_once_with(
        Bucket=default_config.s3_bucket_name, Prefix=expected_scan_prefix, Delimiter='/'
    )

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_list_s3_datasets_various_structures(mock_fetch_public_json, mock_utils_for_dm, mock_s3_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm):
    """Test list_s3_datasets with various valid dataset structures on S3."""
    s3_bucket = "populated-s3-bucket"
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]

    # _get_prefixed_s3_key mock from fixture: lambda key: f"global_prefix/{key}"
    # Base prefix for datasets in S3 - this is what list_s3_datasets uses for its initial scan.
    # scan_base_prefix = default_config.s3_data_prefix.strip('/') + '/' if default_config.s3_data_prefix else ""
    scan_base_prefix = default_config.s3_data_prefix.strip('/') + '/' if default_config.s3_data_prefix else ""
    base_s3_data_prefix = scan_base_prefix

    # Get the mock function for _get_safe_path_component from the fixture dictionary
    get_safe_path_mock = mock_utils_for_dm["_get_safe_path_component"] 

    # Original dataset identifiers
    ds1_id_orig = "user/dataset1-s3"
    ds1_cfg_orig = "default_cfg_s3"
    ds1_rev_orig = "main_s3"

    ds2_id_orig = "microsoft/dataset2"
    ds2_cfg_orig = "custom_s3"
    ds2_rev_orig = "v1.0_s3"

    ds3_id_orig = "huggingface/dataset3"
    ds3_cfg_orig = default_config.default_config_name # Test default config name
    ds3_rev_orig = default_config.default_revision_name # Test default revision name

    # Expected sanitized dataset IDs (what the function will actually return)
    ds1_id_safe = get_safe_path_mock(ds1_id_orig)
    ds2_id_safe = get_safe_path_mock(ds2_id_orig)
    ds3_id_safe = get_safe_path_mock(ds3_id_orig)

    # Form dataset prefixes as they would appear in S3 after _get_s3_prefix (which uses _get_safe_path_component)
    # list_s3_datasets iterates these levels by calling list_objects_v2 multiple times.
    # We need to mock the responses of list_objects_v2 for each level.

    # Level 1: Listing dataset_ids under base_s3_data_prefix
    # _get_s3_prefix internally uses _get_safe_path_component, so the S3 paths reflect that.
    ds1_id_s3_path_segment = get_safe_path_mock(ds1_id_orig)
    ds2_id_s3_path_segment = get_safe_path_mock(ds2_id_orig)
    ds3_id_s3_path_segment = get_safe_path_mock(ds3_id_orig)

    list_level1_response = {
        'CommonPrefixes': [
            {'Prefix': f'{base_s3_data_prefix}{ds1_id_s3_path_segment}/'},
            {'Prefix': f'{base_s3_data_prefix}{ds2_id_s3_path_segment}/'},
            {'Prefix': f'{base_s3_data_prefix}{ds3_id_s3_path_segment}/'},
            {'Prefix': f'{base_s3_data_prefix}not_a_dataset_extra_dir/'} # Extra dir to be ignored
        ]
    }

    # Level 2: Listing config_names under each dataset_id prefix
    ds1_cfg_s3_segment = get_safe_path_mock(ds1_cfg_orig)
    ds2_cfg_s3_segment = get_safe_path_mock(ds2_cfg_orig)
    ds3_cfg_s3_segment = get_safe_path_mock(ds3_cfg_orig) # default

    list_level2_ds1_response = {'CommonPrefixes': [{'Prefix': f'{base_s3_data_prefix}{ds1_id_s3_path_segment}/{ds1_cfg_s3_segment}/'}]}
    list_level2_ds2_response = {'CommonPrefixes': [{'Prefix': f'{base_s3_data_prefix}{ds2_id_s3_path_segment}/{ds2_cfg_s3_segment}/'}]}
    list_level2_ds3_response = {'CommonPrefixes': [{'Prefix': f'{base_s3_data_prefix}{ds3_id_s3_path_segment}/{ds3_cfg_s3_segment}/'}]}
    list_level2_ignored_response = {'CommonPrefixes': []} # For the extra dir

    # Level 3: Listing revisions under each config_name prefix
    ds1_rev_s3_segment = get_safe_path_mock(ds1_rev_orig)
    ds2_rev_s3_segment = get_safe_path_mock(ds2_rev_orig)
    ds3_rev_s3_segment = get_safe_path_mock(ds3_rev_orig) # default_revision

    list_level3_ds1_cfg1_response = {'CommonPrefixes': [{'Prefix': f'{base_s3_data_prefix}{ds1_id_s3_path_segment}/{ds1_cfg_s3_segment}/{ds1_rev_s3_segment}/'}]}
    list_level3_ds2_cfg1_response = {'CommonPrefixes': [{'Prefix': f'{base_s3_data_prefix}{ds2_id_s3_path_segment}/{ds2_cfg_s3_segment}/{ds2_rev_s3_segment}/'}]}
    list_level3_ds3_cfg1_response = {'CommonPrefixes': [{'Prefix': f'{base_s3_data_prefix}{ds3_id_s3_path_segment}/{ds3_cfg_s3_segment}/{ds3_rev_s3_segment}/'}]}



    # Mock paginator behavior - the paginate method returns an iterator of pages
    def mock_paginate_side_effect(Bucket, Prefix, Delimiter):
        assert Bucket == default_config.s3_bucket_name
        assert Delimiter == '/'
        if Prefix == base_s3_data_prefix:
            return iter([list_level1_response])
        # DS1 configs
        elif Prefix == f'{base_s3_data_prefix}{ds1_id_s3_path_segment}/':
            return iter([list_level2_ds1_response])
        # DS1 revisions
        elif Prefix == f'{base_s3_data_prefix}{ds1_id_s3_path_segment}/{ds1_cfg_s3_segment}/':
            return iter([list_level3_ds1_cfg1_response])
        # DS2 configs
        elif Prefix == f'{base_s3_data_prefix}{ds2_id_s3_path_segment}/':
            return iter([list_level2_ds2_response])
        # DS2 revisions
        elif Prefix == f'{base_s3_data_prefix}{ds2_id_s3_path_segment}/{ds2_cfg_s3_segment}/':
            return iter([list_level3_ds2_cfg1_response])
        # DS3 configs
        elif Prefix == f'{base_s3_data_prefix}{ds3_id_s3_path_segment}/':
            return iter([list_level2_ds3_response])
        # DS3 revisions
        elif Prefix == f'{base_s3_data_prefix}{ds3_id_s3_path_segment}/{ds3_cfg_s3_segment}/':
            return iter([list_level3_ds3_cfg1_response])
        elif Prefix == f'{base_s3_data_prefix}not_a_dataset_extra_dir/':
            return iter([list_level2_ignored_response])
        
        print(f"UNMOCKED S3 LIST CALL: Bucket={Bucket}, Prefix={Prefix}") # For debugging
        return iter([{'CommonPrefixes': []}]) # Default for unexpected calls
        
    mock_paginator = mock_s3_utils_for_dm["s3_client_instance"].get_paginator.return_value
    mock_paginator.paginate.side_effect = mock_paginate_side_effect

    # Mock _check_s3_dataset_exists to return True for our test datasets
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True

    # Mock the get_s3_dataset_card_presigned_url from the mock_s3_utils_for_dm fixture
    # This is the one imported into dataset_manager as get_s3_dataset_card_presigned_url
    def mock_presigned_url(**kwargs):
        dataset_id = kwargs.get('dataset_id', 'unknown')
        config_name = kwargs.get('config_name', 'unknown')
        revision = kwargs.get('revision', 'unknown')
        return f"presigned_url_for_{dataset_id}_{config_name}_{revision}"
    
    mock_s3_utils_for_dm["get_s3_dataset_card_presigned_url"].side_effect = mock_presigned_url
    # Also mock the direct patch if used: mock_get_presigned_url_direct (points to the same function in module)
    # mock_get_presigned_url_direct.side_effect = lambda s3_cli, bucket, ds_id, cfg, rev: f"presigned_url_for_{ds_id}_{cfg}_{rev}" # REMOVE THIS LINE


    datasets = list_s3_datasets(config=default_config) # Removed include_card_urls=True, verbose=True
    assert len(datasets) == 3
    datasets_sorted = sorted(datasets, key=lambda d: d["dataset_id"])

    # DS3 assertions (huggingface/dataset3) - first alphabetically
    assert datasets_sorted[0]["dataset_id"] == ds3_id_orig  # Restored from safe name
    assert datasets_sorted[0]["config_name"] == None # default config becomes None
    assert datasets_sorted[0]["revision"] == None # default revision becomes None
    assert datasets_sorted[0]["s3_card_url"] == f"presigned_url_for_{ds3_id_safe}_{ds3_cfg_orig}_{ds3_rev_orig}"

    # DS2 assertions (microsoft/dataset2) - second alphabetically
    assert datasets_sorted[1]["dataset_id"] == ds2_id_orig  # Restored from safe name
    assert datasets_sorted[1]["config_name"] == ds2_cfg_orig
    assert datasets_sorted[1]["revision"] == ds2_rev_orig
    assert datasets_sorted[1]["s3_card_url"] == f"presigned_url_for_{ds2_id_safe}_{ds2_cfg_orig}_{ds2_rev_orig}"

    # DS1 assertions (user/dataset1-s3) - third alphabetically
    assert datasets_sorted[2]["dataset_id"] == ds1_id_orig  # Restored from safe name
    assert datasets_sorted[2]["config_name"] == ds1_cfg_orig
    assert datasets_sorted[2]["revision"] == ds1_rev_orig
    assert datasets_sorted[2]["s3_card_url"] == f"presigned_url_for_{ds1_id_safe}_{ds1_cfg_orig}_{ds1_rev_orig}"

    captured = capsys.readouterr()
    # Print assertions for verbose output removed
    # assert "Found 3 dataset(s) on S3:" in captured.out
    # assert f"Dataset ID: {ds1_id_orig}, Config: {ds1_cfg_orig}, Revision: {ds1_rev_orig}" in captured.out
    # assert "Card on S3: Yes" in captured.out
    # assert "Card URL: presigned_url_for_" in captured.out
    # assert f"Dataset ID: {ds2_id_orig}, Config: {ds2_cfg_orig}, Revision: {ds2_rev_orig}" in captured.out
    # assert "Card on S3: No" in captured.out
    # assert f"Dataset ID: {ds3_id_orig}, Config: {ds3_cfg_orig}, Revision: {ds3_rev_orig}" in captured.out
    
    # Check paginator call count: 
    # 1 call for base_s3_data_prefix
    # 4 calls for dataset_id prefixes (ds1, ds2, ds3, not_a_dataset_extra_dir) 
    # 3 calls for config prefixes (one for each of ds1, ds2, ds3) - not_a_dataset_extra_dir returns empty configs
    # 3 calls for revision prefixes (one for each of ds1, ds2, ds3 config)
    # Total = 1 + 4 + 3 + 0 = 8 calls (not_a_dataset_extra_dir doesn't proceed to revision level)
    assert mock_paginator.paginate.call_count == 8

    # Check get_s3_dataset_card_presigned_url call count: 3 (for all datasets found)
    assert mock_s3_utils_for_dm["get_s3_dataset_card_presigned_url"].call_count == 3

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_list_s3_datasets_client_error_on_list(mock_fetch_public_json, mock_s3_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm):
    s3_bucket = "error-s3-bucket"
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    simulated_error_message = "Simulated Access Denied by Test"
    # s3_client_instance.get_paginator('list_objects_v2') will return a mock paginator
    # That mock paginator's paginate method should be configured to raise an error.
    mock_paginator = mock_s3_utils_for_dm["s3_client_instance"].get_paginator.return_value
    mock_paginator.paginate.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': simulated_error_message}},
        'ListObjectsV2' # Operation name for the ClientError
    )

    datasets = list_s3_datasets(config=default_config)
    assert datasets == []
    captured = capsys.readouterr()
    
    # Check that the function attempted the S3 scan
    assert "Listing S3 datasets via authenticated API call (scanning bucket structure)..." in captured.out
    mock_s3_utils_for_dm["s3_client_instance"].get_paginator.assert_called_once_with('list_objects_v2')
    # Check for the specific error message printed by the except block
    # This assertion needs to be robust to the actual error message from botocore/s3_utils
    assert f"Error listing S3 datasets via API:" in captured.out
    assert simulated_error_message in captured.out # The specific message from the ClientError
    # Check for the fallback messages
    assert "Falling back to check public list if applicable." in captured.out
    assert f"No datasets found in S3 bucket '{default_config.s3_bucket_name}' by scanning or from public list." in captured.out

# --- Tests for sync_local_dataset_to_s3 ---

def test_sync_local_dataset_to_s3_local_not_found(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, capsys):
    """Test sync_local_dataset_to_s3 when local dataset doesn't exist."""
    dataset_id = "nonexistent_dataset"
    config_name = "test_config"
    revision = "v1.0"
    
    success, message = sync_local_dataset_to_s3(dataset_id, config_name, revision, config=default_config)
    
    assert success is False
    expected_path = _get_dataset_path(dataset_id, config_name, revision, config=default_config)
    assert f"Local dataset {dataset_id} (config: {config_name}, revision: {revision}) not found or is incomplete at {expected_path}" in message
    mock_s3_utils_for_dm["_get_s3_client"].assert_not_called()
    captured = capsys.readouterr()
    assert "Cannot sync." in captured.out

def test_sync_local_dataset_to_s3_s3_not_configured(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, capsys):
    """Test sync_local_dataset_to_s3 when S3 is not configured."""
    dataset_id = "test_dataset"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Mock S3 not configured
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None
    config = HGLocalizationConfig(s3_bucket_name=None)
    
    success, message = sync_local_dataset_to_s3(dataset_id, make_public=True, config=default_config)
    
    assert success is False
    assert "S3 not configured" in message
    assert "Cannot make dataset public." in message
    captured = capsys.readouterr()
    assert "S3 not configured" in captured.out

def test_sync_local_dataset_to_s3_already_exists_no_make_public(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 when dataset already exists on S3 and make_public=False."""
    dataset_id = "existing_dataset"
    config_name = "main"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config_name, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    
    success, message = sync_local_dataset_to_s3(dataset_id, config_name, make_public=False, config=default_config)
    
    assert success is True
    assert "completed" in message
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].assert_called_once()
    mock_s3_utils_for_dm["_upload_directory_to_s3"].assert_not_called()
    mock_s3_utils_for_dm["s3_client_instance"].head_object.assert_not_called()
    captured = capsys.readouterr()
    assert "already exists as private S3 copy" in captured.out

def test_sync_local_dataset_to_s3_upload_new_dataset_success(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 when uploading a new dataset successfully."""
    dataset_id = "new_dataset"
    revision = "v2.0"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, revision=revision, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_dict.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = False
    mock_s3_utils_for_dm["_upload_directory_to_s3"].return_value = True
    
    success, message = sync_local_dataset_to_s3(dataset_id, revision=revision, config=default_config)
    
    assert success is True
    assert "completed" in message
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].assert_called_once()
    mock_s3_utils_for_dm["_upload_directory_to_s3"].assert_called_once_with(
        mock_s3_utils_for_dm["s3_client_instance"],
        local_path,
        default_config.s3_bucket_name,
        mock_s3_utils_for_dm["_get_s3_prefix"].return_value
    )
    captured = capsys.readouterr()
    assert "not found on S3 (private). Uploading" in captured.out
    assert "Successfully uploaded dataset" in captured.out

def test_sync_local_dataset_to_s3_upload_failure(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 when S3 upload fails."""
    dataset_id = "upload_fail_dataset"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = False
    mock_s3_utils_for_dm["_upload_directory_to_s3"].side_effect = Exception("S3 upload failed")
    
    success, message = sync_local_dataset_to_s3(dataset_id, config=default_config)
    
    assert success is False
    assert "Error uploading dataset" in message
    assert "S3 upload failed" in message
    captured = capsys.readouterr()
    assert "Error uploading dataset" in captured.out

def test_sync_local_dataset_to_s3_make_public_zip_already_exists(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 with make_public=True when public zip already exists."""
    dataset_id = "public_existing_dataset"
    config_name = "main"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config_name, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    mock_s3_utils_for_dm["_update_public_datasets_json"].return_value = True
    
    # Mock head_object to simulate existing public zip
    mock_s3_utils_for_dm["s3_client_instance"].head_object.return_value = {}
    
    success, message = sync_local_dataset_to_s3(dataset_id, config_name, make_public=True, config=default_config)
    
    assert success is True
    mock_s3_utils_for_dm["s3_client_instance"].head_object.assert_called_once()
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_called_once()
    mock_utils_for_dm["_zip_directory"].assert_not_called()
    captured = capsys.readouterr()
    assert "Public zip" in captured.out
    assert "already exists" in captured.out
    assert "Updating public_datasets.json" in captured.out

@patch('tempfile.TemporaryDirectory')
@patch('tempfile.NamedTemporaryFile')
@patch('shutil.copytree')
def test_sync_local_dataset_to_s3_make_public_create_new_zip_success(
    mock_copytree, mock_tempfile_named, mock_temp_dir,
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 with make_public=True creating new public zip successfully."""
    dataset_id = "public_new_dataset"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    # Removed PUBLIC_DATASETS_ZIP_DIR_PREFIX monkeypatch - using config object instead
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    mock_s3_utils_for_dm["_update_public_datasets_json"].return_value = True
    mock_utils_for_dm["_zip_directory"].return_value = True
    
    # Mock head_object to simulate 404 (zip doesn't exist)
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_utils_for_dm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    # Mock temporary file and directory
    mock_temp_dir_path = temp_datasets_store / "temp_dir"
    mock_temp_dir_path.mkdir()
    mock_temp_dir.return_value.__enter__.return_value = str(mock_temp_dir_path)
    
    mock_temp_file = MagicMock()
    mock_temp_file.name = str(temp_datasets_store / "temp.zip")
    mock_tempfile_named.return_value.__enter__.return_value = mock_temp_file
    
    success, message = sync_local_dataset_to_s3(dataset_id, make_public=True, config=default_config)
    
    assert success is True
    mock_s3_utils_for_dm["s3_client_instance"].head_object.assert_called_once()
    mock_copytree.assert_called_once()
    mock_utils_for_dm["_zip_directory"].assert_called_once()
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.assert_called_once()
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_called_once()
    captured = capsys.readouterr()
    assert "Public zip" in captured.out
    assert "not found. Will attempt to create" in captured.out
    assert "Successfully uploaded public zip" in captured.out

@patch('tempfile.TemporaryDirectory')
@patch('tempfile.NamedTemporaryFile')
@patch('shutil.copytree')
def test_sync_local_dataset_to_s3_make_public_zip_creation_fails(
    mock_copytree, mock_tempfile_named, mock_temp_dir,
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 with make_public=True when zip creation fails."""
    dataset_id = "public_zip_fail_dataset"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    mock_utils_for_dm["_zip_directory"].return_value = False  # Zip creation fails
    
    # Mock head_object to simulate 404 (zip doesn't exist)
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_utils_for_dm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    # Mock temporary file and directory
    mock_temp_dir_path = temp_datasets_store / "temp_dir"
    mock_temp_dir_path.mkdir()
    mock_temp_dir.return_value.__enter__.return_value = str(mock_temp_dir_path)
    
    mock_temp_file = MagicMock()
    mock_temp_file.name = str(temp_datasets_store / "temp.zip")
    mock_tempfile_named.return_value.__enter__.return_value = mock_temp_file
    
    success, message = sync_local_dataset_to_s3(dataset_id, make_public=True, config=default_config)
    
    assert success is True  # Still succeeds because private upload worked
    mock_utils_for_dm["_zip_directory"].assert_called_once()
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.assert_not_called()
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_not_called()
    captured = capsys.readouterr()
    assert "Failed to zip dataset" in captured.out
    assert "Skipping manifest update" in captured.out

@patch('tempfile.TemporaryDirectory')
@patch('tempfile.NamedTemporaryFile')
@patch('shutil.copytree')
def test_sync_local_dataset_to_s3_make_public_upload_fails(
    mock_copytree, mock_tempfile_named, mock_temp_dir,
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 with make_public=True when public zip upload fails."""
    dataset_id = "public_upload_fail_dataset"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    mock_utils_for_dm["_zip_directory"].return_value = True
    
    # Mock head_object to simulate 404 (zip doesn't exist)
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_utils_for_dm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    # Mock upload_file to fail
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.side_effect = Exception("Upload failed")
    
    # Mock temporary file and directory
    mock_temp_dir_path = temp_datasets_store / "temp_dir"
    mock_temp_dir_path.mkdir()
    mock_temp_dir.return_value.__enter__.return_value = str(mock_temp_dir_path)
    
    mock_temp_file = MagicMock()
    mock_temp_file.name = str(temp_datasets_store / "temp.zip")
    mock_tempfile_named.return_value.__enter__.return_value = mock_temp_file
    
    success, message = sync_local_dataset_to_s3(dataset_id, make_public=True, config=default_config)
    
    assert success is True  # Still succeeds because private upload worked
    mock_utils_for_dm["_zip_directory"].assert_called_once()
    mock_s3_utils_for_dm["s3_client_instance"].upload_file.assert_called_once()
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_not_called()
    captured = capsys.readouterr()
    assert "Failed during public zip creation/upload" in captured.out
    assert "Upload failed" in captured.out

def test_sync_local_dataset_to_s3_make_public_head_object_error(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 with make_public=True when head_object returns non-404 error."""
    dataset_id = "public_head_error_dataset"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    
    # Mock head_object to simulate access denied error
    error_response = {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}
    mock_s3_utils_for_dm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    success, message = sync_local_dataset_to_s3(dataset_id, make_public=True, config=default_config)
    
    assert success is True  # Still succeeds because private upload worked
    mock_s3_utils_for_dm["s3_client_instance"].head_object.assert_called_once()
    mock_utils_for_dm["_zip_directory"].assert_not_called()
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_not_called()
    captured = capsys.readouterr()
    assert "Error checking for existing public zip" in captured.out
    assert "Skipping make_public actions" in captured.out

def test_sync_local_dataset_to_s3_make_public_without_private_copy(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 with make_public=True when private S3 copy doesn't exist and upload fails."""
    dataset_id = "public_no_private_dataset"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = False
    mock_s3_utils_for_dm["_upload_directory_to_s3"].side_effect = Exception("Upload failed")
    
    success, message = sync_local_dataset_to_s3(dataset_id, make_public=True, config=default_config)
    
    assert success is False
    assert "Error uploading dataset" in message
    captured = capsys.readouterr()
    assert "Error uploading dataset" in captured.out
    # Should not reach make_public logic since private upload failed

def test_sync_local_dataset_to_s3_make_public_json_update_fails(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_local_dataset_to_s3 with make_public=True when JSON manifest update fails."""
    dataset_id = "public_json_fail_dataset"
    s3_bucket = "test-bucket"
    
    # Create a valid local dataset
    local_path = _get_dataset_path(dataset_id, config=default_config)
    os.makedirs(local_path, exist_ok=True)
    (local_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    mock_s3_utils_for_dm["_update_public_datasets_json"].return_value = False  # Update fails
    
    # Mock head_object to simulate existing public zip
    mock_s3_utils_for_dm["s3_client_instance"].head_object.return_value = {}
    
    success, message = sync_local_dataset_to_s3(dataset_id, make_public=True, config=default_config)
    
    assert success is True  # Still succeeds overall
    mock_s3_utils_for_dm["_update_public_datasets_json"].assert_called_once()
    captured = capsys.readouterr()
    assert "Warning: Failed to update public datasets JSON" in captured.out

# --- Tests for sync_all_local_to_s3 ---

def test_sync_all_local_to_s3_no_local_datasets(temp_datasets_store, capsys):
    """Test sync_all_local_to_s3 when no local datasets exist."""
    sync_all_local_to_s3(make_public=False, config=default_config)
    
    captured = capsys.readouterr()
    assert "Starting sync of all local datasets to S3. Make public: False" in captured.out
    assert "No local datasets found in cache to sync." in captured.out

def test_sync_all_local_to_s3_s3_not_configured(temp_datasets_store, mock_s3_utils_for_dm, capsys):
    """Test sync_all_local_to_s3 when S3 is not configured."""
    # Create a local dataset
    dataset_path = _get_dataset_path("test_dataset", config=default_config)
    os.makedirs(dataset_path, exist_ok=True)
    (dataset_path / "dataset_info.json").touch()
    
    # Mock S3 not configured
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None
    config = HGLocalizationConfig(s3_bucket_name=None)
    
    sync_all_local_to_s3(make_public=True, config=default_config)
    
    captured = capsys.readouterr()
    assert "S3 not configured (bucket name or client init failed). Cannot sync any datasets to S3." in captured.out
    assert "Cannot make datasets public." in captured.out

@patch('hg_localization.dataset_manager.sync_local_dataset_to_s3')
def test_sync_all_local_to_s3_all_success(
    mock_sync_single, temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_all_local_to_s3 when all datasets sync successfully."""
    s3_bucket = "test-bucket"
    
    # Create multiple local datasets
    datasets_info = [
        ("dataset1", None, None),
        ("dataset2", "config1", None),
        ("dataset3", "config2", "v1.0")
    ]
    
    for ds_id, config, revision in datasets_info:
        dataset_path = _get_dataset_path(ds_id, config, revision, config=default_config)
        os.makedirs(dataset_path, exist_ok=True)
        (dataset_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # Mock sync_local_dataset_to_s3 to always succeed
    mock_sync_single.return_value = (True, "Success message")
    
    sync_all_local_to_s3(make_public=True, config=default_config)
    
    # Verify sync_local_dataset_to_s3 was called for each dataset
    assert mock_sync_single.call_count == 3
    expected_calls = [
        call("dataset1", None, None, make_public=True, config=default_config),
        call("dataset2", "config1", None, make_public=True, config=default_config),
        call("dataset3", "config2", "v1.0", make_public=True, config=default_config)
    ]
    mock_sync_single.assert_has_calls(expected_calls, any_order=True)
    
    captured = capsys.readouterr()
    assert "Starting sync of all local datasets to S3. Make public: True" in captured.out
    assert "Total local datasets processed: 3" in captured.out
    assert "Successfully processed (primary sync action): 3" in captured.out
    assert "Failed to process (see logs for errors): 0" in captured.out
    assert "Sync all local datasets to S3 finished" in captured.out

@patch('hg_localization.dataset_manager.sync_local_dataset_to_s3')
def test_sync_all_local_to_s3_mixed_results(
    mock_sync_single, temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_all_local_to_s3 when some datasets succeed and some fail."""
    s3_bucket = "test-bucket"
    
    # Create multiple local datasets
    datasets_info = [
        ("success_dataset1", None, None),
        ("fail_dataset", "config1", None),
        ("success_dataset2", "config2", "v1.0"),
        ("fail_dataset2", None, "v2.0")
    ]
    
    for ds_id, config, revision in datasets_info:
        dataset_path = _get_dataset_path(ds_id, config, revision, config=default_config)
        os.makedirs(dataset_path, exist_ok=True)
        (dataset_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # Mock sync_local_dataset_to_s3 to succeed for some, fail for others
    def mock_sync_side_effect(dataset_id, config_name, revision, make_public, config=None):
        if "fail" in dataset_id:
            return (False, f"Failed to sync {dataset_id}")
        else:
            return (True, f"Successfully synced {dataset_id}")
    
    mock_sync_single.side_effect = mock_sync_side_effect
    
    sync_all_local_to_s3(make_public=False, config=default_config)
    
    # Verify sync_local_dataset_to_s3 was called for each dataset
    assert mock_sync_single.call_count == 4
    
    captured = capsys.readouterr()
    assert "Starting sync of all local datasets to S3. Make public: False" in captured.out
    assert "Total local datasets processed: 4" in captured.out
    assert "Successfully processed (primary sync action): 2" in captured.out
    assert "Failed to process (see logs for errors): 2" in captured.out
    assert "Processing local dataset for sync: ID='success/dataset1'" in captured.out
    assert "Processing local dataset for sync: ID='fail/dataset'" in captured.out

@patch('hg_localization.dataset_manager.sync_local_dataset_to_s3')
def test_sync_all_local_to_s3_all_fail(
    mock_sync_single, temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_all_local_to_s3 when all datasets fail to sync."""
    s3_bucket = "test-bucket"
    
    # Create local datasets
    for i in range(2):
        dataset_path = _get_dataset_path(f"dataset{i}", config=default_config)
        os.makedirs(dataset_path, exist_ok=True)
        (dataset_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # Mock sync_local_dataset_to_s3 to always fail
    mock_sync_single.return_value = (False, "Sync failed")
    
    sync_all_local_to_s3()
    
    assert mock_sync_single.call_count == 2
    
    captured = capsys.readouterr()
    assert "Total local datasets processed: 2" in captured.out
    assert "Successfully processed (primary sync action): 0" in captured.out
    assert "Failed to process (see logs for errors): 2" in captured.out

@patch('hg_localization.dataset_manager.sync_local_dataset_to_s3')
def test_sync_all_local_to_s3_with_various_dataset_structures(
    mock_sync_single, temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test sync_all_local_to_s3 with datasets having various config/revision combinations."""
    s3_bucket = "test-bucket"
    
    # Create datasets with different structures
    datasets_info = [
        ("ds_default_default", default_config.default_config_name, default_config.default_revision_name),
        ("ds_custom_default", "custom_config", default_config.default_revision_name),
        ("ds_default_custom", default_config.default_config_name, "custom_revision"),
        ("ds_custom_custom", "custom_config", "custom_revision")
    ]
    
    for ds_id, config, revision in datasets_info:
        dataset_path = _get_dataset_path(ds_id, config, revision, config=default_config)
        os.makedirs(dataset_path, exist_ok=True)
        (dataset_path / "dataset_dict.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # Mock sync_local_dataset_to_s3 to always succeed
    mock_sync_single.return_value = (True, "Success")
    
    sync_all_local_to_s3(make_public=True, config=default_config)
    
    assert mock_sync_single.call_count == 4
    
    # Verify the calls include proper None values for default config/revision
    calls = mock_sync_single.call_args_list
    call_args = [(call[0][0], call[0][1], call[0][2]) for call in calls]
    
    # list_local_datasets returns None for default config/revision names and restored dataset names
    expected_calls = [
        ("ds_default/default", None, None),
        ("ds_custom/default", "custom_config", None),
        ("ds_default/custom", None, "custom_revision"),
        ("ds_custom/custom", "custom_config", "custom_revision")
    ]
    
    for expected_call in expected_calls:
        assert expected_call in call_args
    
    captured = capsys.readouterr()
    assert "Total local datasets processed: 4" in captured.out
    assert "Successfully processed (primary sync action): 4" in captured.out

def test_sync_all_local_to_s3_verbose_output_format(
    temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys, mock_aws_creds_for_dm
):
    """Test that sync_all_local_to_s3 produces the expected verbose output format."""
    s3_bucket = "test-bucket"
    
    # Create a local dataset
    dataset_path = _get_dataset_path("test_dataset", "test_config", "test_revision", config=default_config)
    os.makedirs(dataset_path, exist_ok=True)
    (dataset_path / "dataset_info.json").touch()
    
    # Configure S3
    # Updated to use config object instead of monkeypatch
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    
    sync_all_local_to_s3(make_public=False, config=default_config)
    
    captured = capsys.readouterr()
    # Check for the specific formatting with escaped newlines
    assert "\\n--- Processing local dataset for sync: ID='test/dataset', Config='test_config', Revision='test_revision' ---" in captured.out
    assert "\\n--- Sync all local datasets to S3 finished ---" in captured.out

# --- Additional tests for public/private cache regression prevention ---

def test_download_dataset_force_public_cache_with_existing_private(
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    """Test that force_public_cache=True bypasses existing private dataset."""
    dataset_id = "test/dataset"
    
    # Create existing private dataset
    private_path = _get_dataset_path(dataset_id, config=default_config, is_public=False)
    os.makedirs(private_path, exist_ok=True)
    (private_path / "dataset_info.json").touch()
    
    # Ensure public path doesn't exist
    public_path = _get_dataset_path(dataset_id, config=default_config, is_public=True)
    assert not public_path.exists()
    
    # Mock S3 not configured to force HF download
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None
    
    # Call download_dataset with force_public_cache=True
    success, result_path = download_dataset(
        dataset_id=dataset_id,
        force_public_cache=True,
        config=default_config
    )
    
    # Should succeed and return public path
    assert success
    assert str(public_path) == result_path
    
    # Should have called HF download despite private dataset existing
    mock_hf_datasets_apis["load_dataset"].assert_called_once()
    mock_hf_datasets_apis["returned_dataset_instance"].save_to_disk.assert_called_once_with(str(public_path))
    
    captured = capsys.readouterr()
    assert "Downloading dataset" in captured.out
    assert "from Hugging Face" in captured.out

def test_download_dataset_make_public_with_existing_private(
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    """Test that make_public=True bypasses existing private dataset."""
    dataset_id = "test/dataset"
    
    # Create existing private dataset
    private_path = _get_dataset_path(dataset_id, config=default_config, is_public=False)
    os.makedirs(private_path, exist_ok=True)
    (private_path / "dataset_info.json").touch()
    
    # Ensure public path doesn't exist
    public_path = _get_dataset_path(dataset_id, config=default_config, is_public=True)
    assert not public_path.exists()
    
    # Mock S3 not configured to force HF download
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None
    
    # Call download_dataset with make_public=True
    success, result_path = download_dataset(
        dataset_id=dataset_id,
        make_public=True,
        config=default_config
    )
    
    # Should succeed and return public path
    assert success
    assert str(public_path) == result_path
    
    # Should have called HF download despite private dataset existing
    mock_hf_datasets_apis["load_dataset"].assert_called_once()
    mock_hf_datasets_apis["returned_dataset_instance"].save_to_disk.assert_called_once_with(str(public_path))

def test_download_dataset_private_prefers_public_over_private(
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    """Test that private download prefers existing public over private."""
    dataset_id = "test/dataset"
    
    # Create both public and private datasets
    private_path = _get_dataset_path(dataset_id, config=default_config, is_public=False)
    public_path = _get_dataset_path(dataset_id, config=default_config, is_public=True)
    
    os.makedirs(private_path, exist_ok=True)
    (private_path / "dataset_info.json").touch()
    
    os.makedirs(public_path, exist_ok=True)
    (public_path / "dataset_info.json").touch()
    
    # Call download_dataset without force_public_cache or make_public
    success, result_path = download_dataset(
        dataset_id=dataset_id,
        config=default_config
    )
    
    # Should return public path (preferred)
    assert success
    assert str(public_path) == result_path
    
    # Should not call HF download since dataset exists locally
    mock_hf_datasets_apis["load_dataset"].assert_not_called()
    
    captured = capsys.readouterr()
    assert "already exists in public cache" in captured.out

def test_download_dataset_private_uses_private_when_no_public(
    temp_datasets_store, mock_hf_datasets_apis, mock_s3_utils_for_dm, 
    mock_utils_for_dm, monkeypatch, capsys
):
    """Test that private download uses private when no public exists."""
    dataset_id = "test/dataset"
    
    # Create only private dataset
    private_path = _get_dataset_path(dataset_id, config=default_config, is_public=False)
    os.makedirs(private_path, exist_ok=True)
    (private_path / "dataset_info.json").touch()
    
    # Ensure public path doesn't exist
    public_path = _get_dataset_path(dataset_id, config=default_config, is_public=True)
    assert not public_path.exists()
    
    # Call download_dataset without force_public_cache or make_public
    success, result_path = download_dataset(
        dataset_id=dataset_id,
        config=default_config
    )
    
    # Should return private path
    assert success
    assert str(private_path) == result_path
    
    # Should not call HF download since dataset exists locally
    mock_hf_datasets_apis["load_dataset"].assert_not_called()
    
    captured = capsys.readouterr()
    assert "already exists in private cache" in captured.out
