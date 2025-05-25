from unittest.mock import MagicMock, patch, call
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

# Import relevant items from other modules that dataset_manager interacts with or uses for config
from hg_localization.config import (
    DATASETS_STORE_PATH, DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME,
    S3_BUCKET_NAME, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    PUBLIC_DATASETS_JSON_KEY, PUBLIC_DATASETS_ZIP_DIR_PREFIX, S3_DATA_PREFIX
)
# Mocks for hf_hub and datasets will be needed for many tests

# --- Fixtures ---

@pytest.fixture
def temp_datasets_store(tmp_path, monkeypatch):
    """Creates a temporary directory for DATASETS_STORE_PATH and patches the config."""
    store_path = tmp_path / "test_datasets_store"
    store_path.mkdir()
    monkeypatch.setattr('hg_localization.dataset_manager.DATASETS_STORE_PATH', store_path)
    monkeypatch.setattr('hg_localization.config.DATASETS_STORE_PATH', store_path)
    return store_path

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
    mock_get_prefixed_s3_key_dm.side_effect = lambda key: f"global_prefix/{key}"
    mock_get_public_url.return_value = "https://mocked.public.url/dataset.zip"

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

# --- Tests for _get_dataset_path (specific to dataset_manager) ---
def test_dm_get_dataset_path(temp_datasets_store, mock_utils_for_dm):
    base_path = temp_datasets_store
    path1 = _get_dataset_path("test/ds1")
    assert path1 == base_path / "test_ds1" / DEFAULT_CONFIG_NAME / DEFAULT_REVISION_NAME
    mock_utils_for_dm["_get_safe_path_component"].assert_any_call("test/ds1")

# --- Tests for _fetch_public_datasets_json_via_url ---

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_no_bucket_name(mock_requests_get, monkeypatch, capsys):
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', None)
    result = _fetch_public_datasets_json_via_url()
    assert result is None
    captured = capsys.readouterr()
    assert "S3_BUCKET_NAME not configured" in captured.out
    mock_requests_get.assert_not_called()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_http_error_not_404(mock_requests_get, mock_s3_utils_for_dm, monkeypatch, capsys):
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', "test-bucket")
    mock_s3_utils_for_dm["_get_prefixed_s3_key"].return_value = PUBLIC_DATASETS_JSON_KEY
    mock_s3_utils_for_dm["_get_s3_public_url"].return_value = "http://dummyurl.com/public_datasets.json"
    
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error", response=MagicMock(status_code=500))
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_datasets_json_via_url()
    assert result is None
    captured = capsys.readouterr()
    assert "HTTP error fetching http://dummyurl.com/public_datasets.json: 500 Server Error" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_http_error_404(mock_requests_get, mock_s3_utils_for_dm, monkeypatch, capsys):
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', "test-bucket")
    mock_s3_utils_for_dm["_get_prefixed_s3_key"].return_value = PUBLIC_DATASETS_JSON_KEY
    mock_s3_utils_for_dm["_get_s3_public_url"].return_value = "http://dummyurl.com/public_datasets.json"

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error", response=MagicMock(status_code=404))
    mock_requests_get.return_value = mock_response

    result = _fetch_public_datasets_json_via_url()
    assert result is None
    captured = capsys.readouterr()
    assert f"{PUBLIC_DATASETS_JSON_KEY} not found at the public URL" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_request_exception(mock_requests_get, mock_s3_utils_for_dm, monkeypatch, capsys):
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', "test-bucket")
    mock_s3_utils_for_dm["_get_prefixed_s3_key"].return_value = PUBLIC_DATASETS_JSON_KEY
    mock_s3_utils_for_dm["_get_s3_public_url"].return_value = "http://dummyurl.com/public_datasets.json"
    
    mock_requests_get.side_effect = requests.exceptions.RequestException("Connection error")
    
    result = _fetch_public_datasets_json_via_url()
    assert result is None
    captured = capsys.readouterr()
    assert "Error fetching http://dummyurl.com/public_datasets.json: Connection error" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_json_decode_error(mock_requests_get, mock_s3_utils_for_dm, monkeypatch, capsys):
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', "test-bucket")
    mock_s3_utils_for_dm["_get_prefixed_s3_key"].return_value = PUBLIC_DATASETS_JSON_KEY
    mock_s3_utils_for_dm["_get_s3_public_url"].return_value = "http://dummyurl.com/public_datasets.json"

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None 
    mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "doc", 0)
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_datasets_json_via_url()
    assert result is None
    captured = capsys.readouterr()
    assert "Error: Content at http://dummyurl.com/public_datasets.json is not valid JSON" in captured.out
    mock_requests_get.assert_called_once()

@patch('hg_localization.dataset_manager.requests.get')
def test_fetch_public_datasets_json_via_url_success(mock_requests_get, mock_s3_utils_for_dm, monkeypatch):
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', "test-bucket")
    mock_s3_utils_for_dm["_get_prefixed_s3_key"].return_value = PUBLIC_DATASETS_JSON_KEY
    mock_s3_utils_for_dm["_get_s3_public_url"].return_value = "http://dummyurl.com/public_datasets.json"

    expected_json = {"key": "value"}
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = expected_json
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_datasets_json_via_url()
    assert result == expected_json
    mock_requests_get.assert_called_once_with("http://dummyurl.com/public_datasets.json", timeout=10)


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
    entry_key = f"{dataset_id}---{config_name}---{DEFAULT_REVISION_NAME}"
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
    entry_key = f"{dataset_id}---{DEFAULT_CONFIG_NAME}---{DEFAULT_REVISION_NAME}"
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
    ds_path = _get_dataset_path(dataset_id, config_name, revision) # Uses patched DATASETS_STORE_PATH
    os.makedirs(ds_path, exist_ok=True)
    local_card_file = ds_path / "dataset_card.md"
    with open(local_card_file, "w", encoding="utf-8") as f:
        f.write(card_content)

    retrieved_content = get_cached_dataset_card_content(dataset_id, config_name, revision)
    assert retrieved_content == card_content
    captured = capsys.readouterr()
    assert f"Found dataset card locally for (dataset: {dataset_id}, config: {config_name}, revision: {revision})" in captured.out

@patch("builtins.open", side_effect=IOError("Read permission denied"))
def test_get_cached_dataset_card_content_local_exists_read_error(mock_open, temp_datasets_store, mock_utils_for_dm, capsys):
    dataset_id = "local_card_io_error"
    ds_path = _get_dataset_path(dataset_id)
    os.makedirs(ds_path, exist_ok=True)
    (ds_path / "dataset_card.md").touch() # File exists

    # Ensure S3 utils are not called by making client None
    with patch('hg_localization.dataset_manager._get_s3_client') as mock_get_s3_cli:
        mock_get_s3_cli.return_value = None
        retrieved_content = get_cached_dataset_card_content(dataset_id)
        assert retrieved_content is None
        captured = capsys.readouterr()
        assert "Error reading local dataset card" in captured.out
        assert "Read permission denied" in captured.out
        mock_open.assert_called_once() # builtins.open was attempted

def test_get_cached_dataset_card_content_s3_success(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys):
    dataset_id = "s3_card_ds"
    config_name = "s3_cfg"
    revision = "s3_rev"
    s3_card_content = "S3 card data!"
    s3_bucket_val = "my-card-bucket"

    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', s3_bucket_val)
    monkeypatch.setattr('hg_localization.config.S3_BUCKET_NAME', s3_bucket_val) 
    # Ensure S3 client is returned
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # _get_s3_prefix is mocked by mock_s3_utils_for_dm to return "mocked/s3/prefix"
    # This prefix is used to construct the s3_card_key
    expected_s3_card_key = "mocked/s3/prefix/dataset_card.md" 

    ds_path = _get_dataset_path(dataset_id, config_name, revision)
    # local_card_file_path will be ds_path / "dataset_card.md"

    def mock_download_file(Bucket, Key, Filename):
        assert Bucket == s3_bucket_val
        assert Key == expected_s3_card_key
        with open(Filename, "w", encoding="utf-8") as f:
            f.write(s3_card_content)
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = mock_download_file

    retrieved_content = get_cached_dataset_card_content(dataset_id, config_name, revision)
    assert retrieved_content == s3_card_content
    mock_s3_utils_for_dm["s3_client_instance"].download_file.assert_called_once()
    assert (ds_path / "dataset_card.md").exists()
    captured = capsys.readouterr()
    assert f"Attempting to download dataset card from S3: s3://{s3_bucket_val}/{expected_s3_card_key}" in captured.out
    assert f"Successfully downloaded dataset card from S3 to {ds_path / 'dataset_card.md'}" in captured.out

def test_get_cached_dataset_card_content_s3_client_error_404(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys):
    dataset_id = "s3_card_404"
    s3_bucket_val = "my-card-bucket-404"
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', s3_bucket_val)
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = ClientError(error_response, 'DownloadFile')
    
    retrieved_content = get_cached_dataset_card_content(dataset_id)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert "Dataset card not found on S3 at mocked/s3/prefix/dataset_card.md" in captured.out

def test_get_cached_dataset_card_content_s3_client_error_other(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys):
    dataset_id = "s3_card_other_error"
    s3_bucket_val = "my-card-bucket-other"
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', s3_bucket_val)
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    error_response = {'Error': {'Code': '500', 'Message': 'Server Error'}}
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = ClientError(error_response, 'DownloadFile')

    retrieved_content = get_cached_dataset_card_content(dataset_id)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert "S3 ClientError when trying to download dataset card mocked/s3/prefix/dataset_card.md" in captured.out

@patch("builtins.open", side_effect=IOError("Post-download read error"))
def test_get_cached_dataset_card_content_s3_download_io_error(mock_open_after_dl, temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys):
    dataset_id = "s3_card_dl_io_error"
    s3_bucket_val = "my-card-bucket-dl-io"
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', s3_bucket_val)
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]

    # Mock download_file to succeed (it creates the file marker)
    def fake_download(Bucket, Key, Filename):
        # Simulate the file being created by download_file, open will be called later to read it.
        Path(Filename).touch() 
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = fake_download

    retrieved_content = get_cached_dataset_card_content(dataset_id)
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
    # Or, alternatively: monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', None)

    retrieved_content = get_cached_dataset_card_content(dataset_id)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert "S3 client not available or bucket not configured. Cannot fetch dataset card from S3." in captured.out

def test_get_cached_dataset_card_content_not_found_anywhere(temp_datasets_store, mock_s3_utils_for_dm, mock_utils_for_dm, monkeypatch, capsys):
    dataset_id = "card_never_found"
    s3_bucket_val = "my-card-bucket-nf"
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', s3_bucket_val)
    mock_s3_utils_for_dm["_get_s3_client"].return_value = mock_s3_utils_for_dm["s3_client_instance"]
    
    # S3 download_file results in 404
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_utils_for_dm["s3_client_instance"].download_file.side_effect = ClientError(error_response, 'DownloadFile')

    retrieved_content = get_cached_dataset_card_content(dataset_id)
    assert retrieved_content is None
    captured = capsys.readouterr()
    assert "Dataset card not found locally" in captured.out
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
        )

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
    s3_bucket_val = "my-s3-bucket-for-dm"
    
    monkeypatch.setattr('hg_localization.config.S3_BUCKET_NAME', s3_bucket_val)
    # Also patch it where dataset_manager imports it if direct import is used
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', s3_bucket_val) 
    monkeypatch.setattr('hg_localization.config.AWS_ACCESS_KEY_ID', "dummy_key_dm")
    monkeypatch.setattr('hg_localization.config.AWS_SECRET_ACCESS_KEY', "dummy_secret_dm")
    # Ensure dataset_manager also sees dummy creds if it imports them directly
    monkeypatch.setattr('hg_localization.dataset_manager.AWS_ACCESS_KEY_ID', "dummy_key_dm")
    monkeypatch.setattr('hg_localization.dataset_manager.AWS_SECRET_ACCESS_KEY', "dummy_secret_dm")

    mock_s3_utils_for_dm["_check_s3_dataset_exists"].return_value = True
    
    local_save_path = _get_dataset_path(dataset_id, config_name) # Uses patched DATASETS_STORE_PATH
    
    def side_effect_s3_download(*args, **kwargs):
        dl_local_path = args[1] 
        os.makedirs(dl_local_path, exist_ok=True)
        (dl_local_path / "dataset_info.json").touch()
        return True
    mock_s3_utils_for_dm["_download_directory_from_s3"].side_effect = side_effect_s3_download
    mock_get_card.return_value = None

    success, msg_path = download_dataset(dataset_id, config_name=config_name)

    assert success is True
    assert Path(msg_path) == local_save_path
    mock_s3_utils_for_dm["_get_s3_client"].assert_called() 
    mock_s3_utils_for_dm["_check_s3_dataset_exists"].assert_called_once() 
    mock_s3_utils_for_dm["_download_directory_from_s3"].assert_called_once_with(
        mock_s3_utils_for_dm["_get_s3_client"].return_value, local_save_path, s3_bucket_val, "mocked/s3/prefix"
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
    dataset_path = _get_dataset_path(dataset_id, config_name=config_name) 
    os.makedirs(dataset_path, exist_ok=True)
    (dataset_path / "dataset_info.json").touch()

    expected_data_obj = mock_hf_datasets_apis["data_loaded_from_disk"]
    loaded_data = load_local_dataset(dataset_id, config_name=config_name)

    assert loaded_data == expected_data_obj
    mock_hf_datasets_apis["load_from_disk"].assert_called_once_with(str(dataset_path))
    captured = capsys.readouterr()
    assert f"Loading dataset '{dataset_id}' (config: {config_name}, revision: default revision) from {dataset_path}" in captured.out

# --- Tests for list_local_datasets (similar to original test_core.py) ---
def test_list_local_datasets_empty(temp_datasets_store, capsys):
    # temp_datasets_store fixture ensures DATASETS_STORE_PATH is set to a clean temp dir
    datasets = list_local_datasets()
    assert datasets == []
    captured = capsys.readouterr()
    assert "No local datasets found in cache." in captured.out

def test_list_local_datasets_with_data(temp_datasets_store, mock_utils_for_dm):
    # Uses _get_dataset_path which in turn uses the mocked _get_safe_path_component
    # and the patched DATASETS_STORE_PATH.
    # We need to make sure _get_safe_path_component is called with the correct parts
    # by _get_dataset_path inside list_local_datasets logic (it is, as list_local_datasets uses the folder names)

    # Dataset 1
    ds1_id_orig = "id/A"
    ds1_cfg_orig = "config/X"
    ds1_rev_orig = "rev/1"
    # Simulate how _get_dataset_path would create them with _get_safe_path_component
    ds1_id_safe = mock_utils_for_dm["_get_safe_path_component"](ds1_id_orig)
    ds1_cfg_safe = mock_utils_for_dm["_get_safe_path_component"](ds1_cfg_orig)
    ds1_rev_safe = mock_utils_for_dm["_get_safe_path_component"](ds1_rev_orig)
    ds1_path = temp_datasets_store / ds1_id_safe / ds1_cfg_safe / ds1_rev_safe
    os.makedirs(ds1_path, exist_ok=True)
    (ds1_path / "dataset_info.json").touch()

    # Dataset 2 (default config, default revision)
    ds2_id_orig = "id_B"
    # For default config/revision, _get_dataset_path uses DEFAULT_CONFIG_NAME/DEFAULT_REVISION_NAME
    # which are then processed by _get_safe_path_component. For this test, assume they are safe.
    ds2_id_safe = mock_utils_for_dm["_get_safe_path_component"](ds2_id_orig)
    ds2_cfg_safe = mock_utils_for_dm["_get_safe_path_component"](DEFAULT_CONFIG_NAME)
    ds2_rev_safe = mock_utils_for_dm["_get_safe_path_component"](DEFAULT_REVISION_NAME)
    ds2_path = temp_datasets_store / ds2_id_safe / ds2_cfg_safe / ds2_rev_safe
    os.makedirs(ds2_path, exist_ok=True)
    (ds2_path / "dataset_dict.json").touch()

    datasets = list_local_datasets()
    assert len(datasets) == 2
    
    # list_local_datasets returns the safe names as dataset_id, config_name, revision
    # And None for config/revision if they match the default *safe* names.
    expected = [
        {"dataset_id": ds1_id_safe, "config_name": ds1_cfg_safe, "revision": ds1_rev_safe},
        {"dataset_id": ds2_id_safe, "config_name": None, "revision": None} # Because they match default safe names
    ]
    
    datasets.sort(key=lambda x: (x["dataset_id"], str(x["config_name"]), str(x["revision"])))
    expected.sort(key=lambda x: (x["dataset_id"], str(x["config_name"]), str(x["revision"])))
    assert datasets == expected

# --- Tests for list_s3_datasets ---

def test_list_s3_datasets_no_bucket_config(monkeypatch, capsys):
    # Directly patch the config variables to None
    monkeypatch.setattr('hg_localization.config.S3_BUCKET_NAME', None)
    monkeypatch.setattr('hg_localization.dataset_manager.S3_BUCKET_NAME', None)
    monkeypatch.setattr('hg_localization.config.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.dataset_manager.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.config.AWS_SECRET_ACCESS_KEY', None)
    monkeypatch.setattr('hg_localization.dataset_manager.AWS_SECRET_ACCESS_KEY', None)

    datasets = list_s3_datasets()
    assert datasets == []
    captured = capsys.readouterr()
    assert "S3_BUCKET_NAME not configured" in captured.out

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_list_s3_datasets_no_creds_fallback_to_public_json_empty(
    mock_fetch_public_json, mock_s3_utils_for_dm, monkeypatch, capsys
):
    s3_bucket_val = "public-list-bucket"
    monkeypatch.setattr('hg_localization.config.S3_BUCKET_NAME', s3_bucket_val)
    monkeypatch.setattr('hg_localization.config.AWS_ACCESS_KEY_ID', None) # No creds
    monkeypatch.setattr('hg_localization.config.AWS_SECRET_ACCESS_KEY', None)
    
    # Ensure _get_s3_client inside list_s3_datasets returns None when no creds
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None
    mock_fetch_public_json.return_value = None # Public JSON also not found or empty

    datasets = list_s3_datasets()
    assert datasets == []
    captured = capsys.readouterr()
    assert "Attempting to list S3 datasets from public_datasets.json..." in captured.out
    assert f"Could not fetch or parse {PUBLIC_DATASETS_JSON_KEY}" in captured.out
    mock_fetch_public_json.assert_called_once()

@patch('hg_localization.dataset_manager._fetch_public_datasets_json_via_url')
def test_list_s3_datasets_no_creds_fallback_to_public_json_with_data(
    mock_fetch_public_json, mock_s3_utils_for_dm, monkeypatch, capsys
):
    s3_bucket_val = "public-list-bucket-data"
    monkeypatch.setattr('hg_localization.config.S3_BUCKET_NAME', s3_bucket_val)
    monkeypatch.setattr('hg_localization.config.AWS_ACCESS_KEY_ID', None)
    monkeypatch.setattr('hg_localization.config.AWS_SECRET_ACCESS_KEY', None)
    mock_s3_utils_for_dm["_get_s3_client"].return_value = None

    public_json_data = {
        "ds1---cfgA---revB": {"dataset_id": "ds1", "config_name": "cfgA", "revision": "revB", "s3_zip_key": "..."},
        "ds2---default_config---default_revision": {"dataset_id": "ds2", "s3_zip_key": "..."} # Config/rev are None if default
    }
    mock_fetch_public_json.return_value = public_json_data

    datasets = list_s3_datasets()
    assert len(datasets) == 2
    expected = [
        {"dataset_id": "ds1", "config_name": "cfgA", "revision": "revB", "s3_card_url": None},
        {"dataset_id": "ds2", "config_name": None, "revision": None, "s3_card_url": None}
    ]
    datasets.sort(key=lambda x: (x["dataset_id"], str(x["config_name"]), str(x["revision"])))
    expected.sort(key=lambda x: (x["dataset_id"], str(x["config_name"]), str(x["revision"])))
    assert datasets == expected
    captured = capsys.readouterr()
    assert "Listing S3 datasets based on public_datasets.json." in captured.out

# More complex tests for list_s3_datasets with S3 client (mocking paginator) should be added.
# These would be similar to how they might have been in test_core.py but now using
# mock_s3_utils_for_dm["s3_client_instance"] and its paginator.
