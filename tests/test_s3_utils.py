import pytest
from unittest.mock import patch, MagicMock, call, ANY
from pathlib import Path
import json

from botocore.exceptions import NoCredentialsError, ClientError
from botocore.config import Config

# Functions to test from s3_utils.py
from hg_localization.s3_utils import (
    _get_s3_client,
    _get_s3_prefix,
    _get_prefixed_s3_key,
    _check_s3_dataset_exists,
    _upload_directory_to_s3,
    _download_directory_from_s3,
    _update_public_datasets_json,
    _get_s3_public_url,
    get_s3_dataset_card_presigned_url
)

# Config values that might be monkeypatched
from hg_localization.config import (
    S3_BUCKET_NAME, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY, S3_DATA_PREFIX,
    DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME,
    PUBLIC_DATASETS_JSON_KEY
)

@pytest.fixture
def mock_boto3_client(monkeypatch):
    mock_client_instance = MagicMock()
    mock_boto_client_constructor = MagicMock(return_value=mock_client_instance)
    monkeypatch.setattr("boto3.client", mock_boto_client_constructor)
    return {"constructor": mock_boto_client_constructor, "instance": mock_client_instance}

@pytest.fixture
def mock_utils_for_s3(mocker):
    mock_get_safe_path = mocker.patch('hg_localization.s3_utils._get_safe_path_component')
    mock_get_safe_path.side_effect = lambda name: name.replace("/", "_").replace("\\", "_") if name else ""
    return {"_get_safe_path_component": mock_get_safe_path}


# --- Tests for _get_s3_client ---

def test_get_s3_client_no_bucket_name(monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", None)
    client = _get_s3_client()
    assert client is None

def test_get_s3_client_no_aws_keys(monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", None)
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "secret")
    client = _get_s3_client()
    assert client is None
    
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", None)
    client2 = _get_s3_client()
    assert client2 is None

def test_get_s3_client_success(mock_boto3_client, monkeypatch):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "test-key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setattr("hg_localization.s3_utils.S3_ENDPOINT_URL", "http://localhost:9000")

    mock_boto3_client["instance"].head_bucket.return_value = {}

    client = _get_s3_client()
    assert client == mock_boto3_client["instance"]
    mock_boto3_client["constructor"].assert_called_once_with(
        's3',
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        endpoint_url="http://localhost:9000",
        config=ANY
    )
    mock_boto3_client["instance"].head_bucket.assert_called_once_with(Bucket="test-bucket")

def test_get_s3_client_boto_raises_no_credentials(mock_boto3_client, monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "secret")
    
    mock_boto3_client["constructor"].side_effect = NoCredentialsError()
    client = _get_s3_client()
    assert client is None
    captured = capsys.readouterr()
    assert "S3 Error: AWS credentials not found" in captured.out

def test_get_s3_client_head_bucket_no_such_bucket(mock_boto3_client, monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "non-existent-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "secret")
    
    error_response = {'Error': {'Code': 'NoSuchBucket', 'Message': 'The specified bucket does not exist'}}
    mock_boto3_client["instance"].head_bucket.side_effect = ClientError(error_response, 'HeadBucket')
    
    client = _get_s3_client()
    assert client is None
    captured = capsys.readouterr()
    assert "S3 Error: Bucket 'non-existent-bucket' does not exist." in captured.out

def test_get_s3_client_head_bucket_invalid_access_key(mock_boto3_client, monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "invalid-key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "secret")

    error_response = {'Error': {'Code': 'InvalidAccessKeyId', 'Message': '...'}}
    mock_boto3_client["instance"].head_bucket.side_effect = ClientError(error_response, 'HeadBucket')

    client = _get_s3_client()
    assert client is None
    captured = capsys.readouterr()
    assert "S3 Error: Invalid AWS credentials provided." in captured.out

def test_get_s3_client_head_bucket_signature_mismatch(mock_boto3_client, monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "wrong-secret")

    error_response = {'Error': {'Code': 'SignatureDoesNotMatch', 'Message': '...'}}
    mock_boto3_client["instance"].head_bucket.side_effect = ClientError(error_response, 'HeadBucket')

    client = _get_s3_client()
    assert client is None
    captured = capsys.readouterr()
    assert "S3 Error: Invalid AWS credentials provided." in captured.out

def test_get_s3_client_head_bucket_other_client_error(mock_boto3_client, monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "secret")

    error_response = {'Error': {'Code': 'SomeOtherError', 'Message': 'Details...'}}
    mock_boto3_client["instance"].head_bucket.side_effect = ClientError(error_response, 'HeadBucket')

    client = _get_s3_client()
    assert client is None
    captured = capsys.readouterr()
    assert "S3 ClientError during client test: An error occurred (SomeOtherError)" in captured.out

def test_get_s3_client_generic_exception(mock_boto3_client, monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "test-bucket")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setattr("hg_localization.s3_utils.AWS_SECRET_ACCESS_KEY", "secret")
    
    mock_boto3_client["constructor"].side_effect = Exception("Unexpected error")
    client = _get_s3_client()
    assert client is None
    captured = capsys.readouterr()
    assert "Error initializing S3 client: Unexpected error" in captured.out

# --- Tests for _get_s3_prefix ---

@pytest.mark.parametrize("dataset_id, config_name, revision, s3_data_prefix_val, expected_prefix", [
    ("my_dataset", "config1", "revA", "", "my_dataset/config1/revA"),
    ("user/ds-name", "conf/1", "v1.0.0", "", "user_ds-name/conf_1/v1.0.0"),
    ("dataset_id", None, None, "", f"dataset_id/{DEFAULT_CONFIG_NAME}/{DEFAULT_REVISION_NAME}"),
    ("dataset_id", "config1", None, "", f"dataset_id/config1/{DEFAULT_REVISION_NAME}"),
    ("dataset_id", None, "revA", "", f"dataset_id/{DEFAULT_CONFIG_NAME}/revA"),
    ("my_ds", "cfg", "rev", "data/prod", "data/prod/my_ds/cfg/rev"),
    ("my_ds", "cfg", "rev", "data/prod/", "data/prod/my_ds/cfg/rev"),
    ("my_ds", "cfg", "rev", "/data/prod", "data/prod/my_ds/cfg/rev"),
])
def test_get_s3_prefix_various_inputs(mock_utils_for_s3, monkeypatch, dataset_id, config_name, revision, s3_data_prefix_val, expected_prefix):
    monkeypatch.setattr("hg_localization.s3_utils.S3_DATA_PREFIX", s3_data_prefix_val)
    monkeypatch.setattr("hg_localization.config.DEFAULT_CONFIG_NAME", DEFAULT_CONFIG_NAME)
    monkeypatch.setattr("hg_localization.config.DEFAULT_REVISION_NAME", DEFAULT_REVISION_NAME)

    prefix = _get_s3_prefix(dataset_id, config_name, revision)
    assert prefix == expected_prefix

    mock_utils_for_s3["_get_safe_path_component"].assert_any_call(dataset_id)
    cfg_to_check = config_name if config_name else DEFAULT_CONFIG_NAME
    mock_utils_for_s3["_get_safe_path_component"].assert_any_call(cfg_to_check)
    rev_to_check = revision if revision else DEFAULT_REVISION_NAME
    mock_utils_for_s3["_get_safe_path_component"].assert_any_call(rev_to_check)

# --- Tests for _get_prefixed_s3_key ---
@pytest.mark.parametrize("base_key, s3_data_prefix_val, expected_key", [
    ("my_file.txt", "prod_data", "prod_data/my_file.txt"),
    ("my_file.txt", "prod_data/", "prod_data/my_file.txt"),
    ("/my_file.txt", "prod_data", "prod_data/my_file.txt"),
    ("my_file.txt", "", "my_file.txt"),
    ("/other/file.json", "", "other/file.json"),
])
def test_get_prefixed_s3_key(monkeypatch, base_key, s3_data_prefix_val, expected_key):
    monkeypatch.setattr("hg_localization.s3_utils.S3_DATA_PREFIX", s3_data_prefix_val)
    key = _get_prefixed_s3_key(base_key)
    assert key == expected_key

# --- Tests for _check_s3_dataset_exists ---
def test_check_s3_dataset_exists_no_client_or_bucket(mock_boto3_client):
    assert _check_s3_dataset_exists(None, "bucket", "prefix") is False
    assert _check_s3_dataset_exists(mock_boto3_client["instance"], None, "prefix") is False

def test_check_s3_dataset_exists_info_json_exists(mock_boto3_client):
    mock_client = mock_boto3_client["instance"]
    mock_client.head_object.return_value = {}
    assert _check_s3_dataset_exists(mock_client, "bucket", "prefix/ds_v1") is True
    mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="prefix/ds_v1/dataset_info.json")

def test_check_s3_dataset_exists_dict_json_exists(mock_boto3_client):
    mock_client = mock_boto3_client["instance"]
    error_404 = ClientError({'Error': {'Code': '404'}}, 'HeadObject')
    mock_client.head_object.side_effect = [error_404, {}]
    assert _check_s3_dataset_exists(mock_client, "bucket", "prefix/ds_v2") is True
    expected_calls = [
        call(Bucket="bucket", Key="prefix/ds_v2/dataset_info.json"),
        call(Bucket="bucket", Key="prefix/ds_v2/dataset_dict.json")
    ]
    mock_client.head_object.assert_has_calls(expected_calls)

def test_check_s3_dataset_exists_neither_json_exists(mock_boto3_client):
    mock_client = mock_boto3_client["instance"]
    error_404 = ClientError({'Error': {'Code': '404'}}, 'HeadObject')
    mock_client.head_object.side_effect = [error_404, error_404]
    assert _check_s3_dataset_exists(mock_client, "bucket", "prefix/ds_v3") is False
    expected_calls = [
        call(Bucket="bucket", Key="prefix/ds_v3/dataset_info.json"),
        call(Bucket="bucket", Key="prefix/ds_v3/dataset_dict.json")
    ]
    mock_client.head_object.assert_has_calls(expected_calls)

def test_check_s3_dataset_exists_other_client_error(mock_boto3_client):
    mock_client = mock_boto3_client["instance"]
    error_other = ClientError({'Error': {'Code': 'SomeOtherError'}}, 'HeadObject')
    mock_client.head_object.side_effect = error_other
    assert _check_s3_dataset_exists(mock_client, "bucket", "prefix/ds_v4") is False
    mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="prefix/ds_v4/dataset_info.json")

def test_check_s3_dataset_exists_generic_exception(mock_boto3_client):
    mock_client = mock_boto3_client["instance"]
    mock_client.head_object.side_effect = Exception("Unexpected error")
    assert _check_s3_dataset_exists(mock_client, "bucket", "prefix/ds_v5") is False
    mock_client.head_object.assert_called_once_with(Bucket="bucket", Key="prefix/ds_v5/dataset_info.json")

# --- Tests for _upload_directory_to_s3 ---

@pytest.fixture
def temp_local_dir_for_upload(tmp_path):
    upload_src = tmp_path / "upload_source"
    upload_src.mkdir()
    (upload_src / "file1.txt").write_text("content1")
    sub_dir = upload_src / "subdir"
    sub_dir.mkdir()
    (sub_dir / "file2.json").write_text("{\"key\": \"value\"}")
    return upload_src

def test_upload_directory_to_s3_success(mock_boto3_client, temp_local_dir_for_upload, capsys):
    mock_client = mock_boto3_client["instance"]
    bucket = "upload-bucket"
    prefix = "datasets/my_ds_upload"

    _upload_directory_to_s3(mock_client, temp_local_dir_for_upload, bucket, prefix)

    expected_calls = [
        call(str(temp_local_dir_for_upload / "file1.txt"), bucket, f"{prefix}/file1.txt"),
        call(str(temp_local_dir_for_upload / "subdir" / "file2.json"), bucket, f"{prefix}/subdir/file2.json")
    ]
    mock_client.upload_file.assert_has_calls(expected_calls, any_order=True)
    assert mock_client.upload_file.call_count == 2
    captured = capsys.readouterr()
    assert f"Uploading {temp_local_dir_for_upload} to s3://{bucket}/{prefix}..." in captured.out
    assert "Upload complete." in captured.out

def test_upload_directory_to_s3_one_file_fails(mock_boto3_client, temp_local_dir_for_upload, capsys):
    mock_client = mock_boto3_client["instance"]
    bucket = "upload-fail-bucket"
    prefix = "uploads/errors"

    file1_path_str = str(temp_local_dir_for_upload / "file1.txt")
    file2_path_str = str(temp_local_dir_for_upload / "subdir" / "file2.json")

    def upload_side_effect(local_path, s3_bucket, s3_key):
        if local_path == file1_path_str:
            raise Exception("Upload failed for file1")
        # For file2, default mock behavior (no error)
    mock_client.upload_file.side_effect = upload_side_effect

    _upload_directory_to_s3(mock_client, temp_local_dir_for_upload, bucket, prefix)

    # Check that upload_file was attempted for both
    expected_attempts = [
        call(file1_path_str, bucket, f"{prefix}/file1.txt"),
        call(file2_path_str, bucket, f"{prefix}/subdir/file2.json")
    ]
    mock_client.upload_file.assert_has_calls(expected_attempts, any_order=True)
    assert mock_client.upload_file.call_count == 2
    captured = capsys.readouterr()
    assert "Failed to upload file1.txt: Upload failed for file1" in captured.out
    assert f"Uploaded file2.json to {prefix}/subdir/file2.json" in captured.out

def test_upload_directory_to_s3_empty_dir(mock_boto3_client, tmp_path, capsys):
    mock_client = mock_boto3_client["instance"]
    empty_dir = tmp_path / "empty_source"
    empty_dir.mkdir()
    bucket = "empty-upload-bucket"
    prefix = "empty_prefix"

    _upload_directory_to_s3(mock_client, empty_dir, bucket, prefix)
    mock_client.upload_file.assert_not_called()
    captured = capsys.readouterr()
    assert "Upload complete." in captured.out # Still prints complete


# --- Tests for _download_directory_from_s3 ---

@pytest.fixture
def temp_local_dir_for_download(tmp_path):
    download_target = tmp_path / "download_target"
    # No need to mkdir, function should do it
    return download_target

def test_download_directory_from_s3_success(mock_boto3_client, temp_local_dir_for_download, capsys):
    mock_client = mock_boto3_client["instance"]
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    bucket = "download-bucket"
    prefix = "data/to_download"

    s3_objects_page1 = [
        {'Key': f'{prefix}/file1.txt'},
        {'Key': f'{prefix}/folder1/'} # Directory marker, should be skipped
    ]
    s3_objects_page2 = [
        {'Key': f'{prefix}/folder1/file2.csv'}
    ]
    mock_paginator.paginate.return_value = iter([
        {'Contents': s3_objects_page1},
        {'Contents': s3_objects_page2}
    ])

    # Simulate file creation by download_file
    def mock_download_side_effect(bucket_name, s3_key, local_path_str):
        local_path = Path(local_path_str)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.touch()
    mock_client.download_file.side_effect = mock_download_side_effect

    success = _download_directory_from_s3(mock_client, temp_local_dir_for_download, bucket, prefix)

    assert success is True
    assert (temp_local_dir_for_download / "file1.txt").exists()
    assert (temp_local_dir_for_download / "folder1" / "file2.csv").exists()

    expected_download_calls = [
        call(bucket, f"{prefix}/file1.txt", str(temp_local_dir_for_download / "file1.txt")),
        call(bucket, f"{prefix}/folder1/file2.csv", str(temp_local_dir_for_download / "folder1" / "file2.csv"))
    ]
    mock_client.download_file.assert_has_calls(expected_download_calls, any_order=True)
    assert mock_client.download_file.call_count == 2
    captured = capsys.readouterr()
    assert f"Attempting to download s3://{bucket}/{prefix} to {temp_local_dir_for_download}..." in captured.out
    assert "Successfully downloaded 2 files from S3." in captured.out

def test_download_directory_from_s3_no_contents(mock_boto3_client, temp_local_dir_for_download, capsys):
    mock_client = mock_boto3_client["instance"]
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = iter([{'SomeOtherKey': []}]) # No 'Contents' key
    bucket = "dl-no-contents-bucket"
    prefix = "empty_data"

    success = _download_directory_from_s3(mock_client, temp_local_dir_for_download, bucket, prefix)
    assert success is False
    mock_client.download_file.assert_not_called()
    captured = capsys.readouterr()
    assert f"No objects found in s3://{bucket}/{prefix}" in captured.out

def test_download_directory_from_s3_no_files_downloaded_empty_contents(mock_boto3_client, temp_local_dir_for_download, capsys):
    mock_client = mock_boto3_client["instance"]
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = iter([{'Contents': []}]) # Empty Contents list
    bucket = "dl-empty-contents-bucket"
    prefix = "data/no_files"

    success = _download_directory_from_s3(mock_client, temp_local_dir_for_download, bucket, prefix)
    assert success is False # Returns False if files_downloaded is 0
    mock_client.download_file.assert_not_called()
    captured = capsys.readouterr()
    assert f"No files were actually downloaded from s3://{bucket}/{prefix}." in captured.out

def test_download_directory_from_s3_download_file_client_error(mock_boto3_client, temp_local_dir_for_download, capsys):
    mock_client = mock_boto3_client["instance"]
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    bucket = "dl-client-error-bucket"
    prefix = "data/err"

    s3_objects = [{'Key': f'{prefix}/file_to_fail.txt'}]
    mock_paginator.paginate.return_value = iter([{'Contents': s3_objects}])
    mock_client.download_file.side_effect = ClientError({'Error': {'Code': 'AccessDenied'}}, 'DownloadFile')

    success = _download_directory_from_s3(mock_client, temp_local_dir_for_download, bucket, prefix)
    assert success is False
    mock_client.download_file.assert_called_once()
    captured = capsys.readouterr()
    assert f"S3 Error during download from prefix '{prefix}': An error occurred (AccessDenied)" in captured.out

def test_download_directory_from_s3_generic_exception_during_download(mock_boto3_client, temp_local_dir_for_download, capsys):
    mock_client = mock_boto3_client["instance"]
    mock_paginator = MagicMock()
    mock_client.get_paginator.return_value = mock_paginator
    bucket = "dl-generic-err-bucket"
    prefix = "data/generic_fail"

    s3_objects = [{'Key': f'{prefix}/another_file.txt'}]
    mock_paginator.paginate.return_value = iter([{'Contents': s3_objects}])
    mock_client.download_file.side_effect = Exception("Disk full or something")

    success = _download_directory_from_s3(mock_client, temp_local_dir_for_download, bucket, prefix)
    assert success is False
    captured = capsys.readouterr()
    assert f"Error downloading from S3 prefix '{prefix}': Disk full or something" in captured.out 

# --- Tests for _update_public_datasets_json ---

def test_update_public_datasets_json_no_s3_client(capsys):
    assert _update_public_datasets_json(None, "bucket", "ds_id", "cfg", "rev", "zip_key") is False
    # No print from this specific early exit condition

@patch("hg_localization.s3_utils._get_prefixed_s3_key")
def test_update_public_datasets_json_creates_new_if_not_exists(mock_get_prefixed_key, mock_boto3_client, monkeypatch, capsys):
    mock_client = mock_boto3_client["instance"]
    bucket = "json-update-bucket"
    dataset_id = "new_ds_in_json"
    config_name = "config_new"
    revision = "rev_new"
    zip_key = "path/to/new_ds.zip"
    public_json_s3_key = "global_prefix/public_datasets.json"

    mock_get_prefixed_key.return_value = public_json_s3_key
    monkeypatch.setattr("hg_localization.s3_utils.PUBLIC_DATASETS_JSON_KEY", "public_datasets.json")
    monkeypatch.setattr("hg_localization.config.DEFAULT_CONFIG_NAME", "default_config") # For entry key construction
    monkeypatch.setattr("hg_localization.config.DEFAULT_REVISION_NAME", "default_revision")

    error_no_such_key = ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
    mock_client.get_object.side_effect = error_no_such_key
    mock_client.put_object.return_value = {} # Simulate successful put

    success = _update_public_datasets_json(mock_client, bucket, dataset_id, config_name, revision, zip_key)
    assert success is True

    mock_client.get_object.assert_called_once_with(Bucket=bucket, Key=public_json_s3_key)
    
    expected_entry_key = f"{dataset_id}---{config_name}---{revision}"
    expected_json_content = {
        expected_entry_key: {
            "dataset_id": dataset_id,
            "config_name": config_name,
            "revision": revision,
            "s3_zip_key": zip_key,
            "s3_bucket": bucket
        }
    }
    mock_client.put_object.assert_called_once_with(
        Bucket=bucket,
        Key=public_json_s3_key,
        Body=json.dumps(expected_json_content, indent=2),
        ContentType='application/json',
        ACL='public-read'
    )
    captured = capsys.readouterr()
    assert f"{public_json_s3_key} not found in S3, will create a new one." in captured.out
    assert f"Successfully updated and published {public_json_s3_key} in S3." in captured.out

@patch("hg_localization.s3_utils._get_prefixed_s3_key")
def test_update_public_datasets_json_updates_existing(mock_get_prefixed_key, mock_boto3_client, monkeypatch, capsys):
    mock_client = mock_boto3_client["instance"]
    bucket = "json-update-existing-bucket"
    dataset_id = "existing_ds"
    config_name = None # Test default name usage
    revision = "rev_updated"
    zip_key = "path/to/updated.zip"
    public_json_s3_key = "global_prefix/public_manifest.json"

    mock_get_prefixed_key.return_value = public_json_s3_key
    monkeypatch.setattr("hg_localization.s3_utils.PUBLIC_DATASETS_JSON_KEY", "public_manifest.json")
    # Patch default names from the *config* module, as that's where _update_public_datasets_json imports them from
    monkeypatch.setattr("hg_localization.config.DEFAULT_CONFIG_NAME", "default_cfg_name_test")
    monkeypatch.setattr("hg_localization.config.DEFAULT_REVISION_NAME", "default_rev_name_test")

    initial_json_content = {
        "other_ds---default---v1": {"dataset_id": "other_ds", "s3_zip_key": "...", "s3_bucket": bucket}
    }
    mock_response_body = MagicMock()
    mock_response_body.read.return_value = json.dumps(initial_json_content).encode('utf-8')
    mock_client.get_object.return_value = {'Body': mock_response_body}
    mock_client.put_object.return_value = {}

    success = _update_public_datasets_json(mock_client, bucket, dataset_id, config_name, revision, zip_key)
    assert success is True

    # Use the monkeypatched DEFAULT_CONFIG_NAME for expected_entry_key
    # The function _update_public_datasets_json will use the value "default_cfg_name_test"
    # because it imports DEFAULT_CONFIG_NAME from the patched hg_localization.config module.
    expected_entry_key = f"{dataset_id}---{"default_cfg_name_test"}---{revision}"
    updated_json_content = initial_json_content.copy()
    updated_json_content[expected_entry_key] = {
        "dataset_id": dataset_id,
        "config_name": config_name, # Will be None
        "revision": revision,
        "s3_zip_key": zip_key,
        "s3_bucket": bucket
    }
    mock_client.put_object.assert_called_once_with(
        Bucket=bucket,
        Key=public_json_s3_key,
        Body=json.dumps(updated_json_content, indent=2),
        ContentType='application/json',
        ACL='public-read'
    )

@patch("hg_localization.s3_utils._get_prefixed_s3_key")
def test_update_public_datasets_json_corrupted_json(mock_get_prefixed_key, mock_boto3_client, monkeypatch, capsys):
    mock_client = mock_boto3_client["instance"]
    bucket = "json-corrupt-bucket"
    public_json_s3_key = "global/corrupt.json"
    mock_get_prefixed_key.return_value = public_json_s3_key
    monkeypatch.setattr("hg_localization.s3_utils.PUBLIC_DATASETS_JSON_KEY", "corrupt.json")
    monkeypatch.setattr("hg_localization.config.DEFAULT_CONFIG_NAME", "def_cfg")
    monkeypatch.setattr("hg_localization.config.DEFAULT_REVISION_NAME", "def_rev")

    mock_response_body = MagicMock()
    mock_response_body.read.return_value = b"this is not json"
    mock_client.get_object.return_value = {'Body': mock_response_body}
    mock_client.put_object.return_value = {}

    dataset_id, cfg, rev, zip_k = "ds_replacing_corrupt", "c1", "r1", "p/z.zip"
    success = _update_public_datasets_json(mock_client, bucket, dataset_id, cfg, rev, zip_k)
    assert success is True

    expected_entry_key = f"{dataset_id}---{cfg}---{rev}"
    expected_json_content = {
        expected_entry_key: {
            "dataset_id": dataset_id, "config_name": cfg, "revision": rev,
            "s3_zip_key": zip_k, "s3_bucket": bucket
        }
    }
    mock_client.put_object.assert_called_once_with(
        Bucket=bucket, Key=public_json_s3_key,
        Body=json.dumps(expected_json_content, indent=2),
        ContentType='application/json', ACL='public-read'
    )
    captured = capsys.readouterr()
    assert f"Error: {public_json_s3_key} in S3 is corrupted. Will overwrite." in captured.out

@patch("hg_localization.s3_utils._get_prefixed_s3_key")
def test_update_public_datasets_json_get_object_client_error(mock_get_prefixed_key, mock_boto3_client, monkeypatch, capsys):
    mock_client = mock_boto3_client["instance"]
    public_json_s3_key = "global/public.json"
    mock_get_prefixed_key.return_value = public_json_s3_key
    monkeypatch.setattr("hg_localization.s3_utils.PUBLIC_DATASETS_JSON_KEY", "public.json")
    
    error_access_denied = ClientError({'Error': {'Code': 'AccessDenied'}}, 'GetObject')
    mock_client.get_object.side_effect = error_access_denied

    success = _update_public_datasets_json(mock_client, "bucket", "ds", "c", "r", "z.zip")
    assert success is False
    mock_client.put_object.assert_not_called()
    captured = capsys.readouterr()
    assert f"Error fetching {public_json_s3_key} from S3: An error occurred (AccessDenied)" in captured.out

@patch("hg_localization.s3_utils._get_prefixed_s3_key")
def test_update_public_datasets_json_put_object_fails(mock_get_prefixed_key, mock_boto3_client, monkeypatch, capsys):
    mock_client = mock_boto3_client["instance"]
    public_json_s3_key = "global_prefix/manifest.json"
    mock_get_prefixed_key.return_value = public_json_s3_key
    monkeypatch.setattr("hg_localization.s3_utils.PUBLIC_DATASETS_JSON_KEY", "manifest.json")
    monkeypatch.setattr("hg_localization.config.DEFAULT_CONFIG_NAME", "default_config") 
    monkeypatch.setattr("hg_localization.config.DEFAULT_REVISION_NAME", "default_revision")

    # Simulate get_object succeeding (e.g., file doesn't exist, so current_config is empty)
    error_no_such_key = ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
    mock_client.get_object.side_effect = error_no_such_key
    mock_client.put_object.side_effect = ClientError({'Error': {'Code': 'InternalError'}}, 'PutObject')

    success = _update_public_datasets_json(mock_client, "put-fail-bucket", "ds_put_fail", "cfg", "rev", "zip.zip")
    assert success is False
    mock_client.put_object.assert_called_once() # It was attempted
    captured = capsys.readouterr()
    assert f"Error uploading {public_json_s3_key} to S3: An error occurred (InternalError)" in captured.out 

# --- Tests for _get_s3_public_url ---

@pytest.mark.parametrize("bucket, key, endpoint, expected_url", [
    ("my-bucket", "data/file.zip", "https://minio.example.com", "https://my-bucket.minio.example.com/data/file.zip"),
    ("another.bucket", "/deep/path/obj.dat", "http://s3.local:9000/", "http://another.bucket.s3.local:9000/deep/path/obj.dat"),
    ("my-bucket", "file.zip", None, "https://my-bucket.s3.amazonaws.com/file.zip"),
    ("my-bucket", "/file.zip", None, "https://my-bucket.s3.amazonaws.com/file.zip"), # Leading slash in key
    ("test-bucket", "test.zip", "s3.mycustom.com", "https://test-bucket.s3.mycustom.com/test.zip") # Endpoint without scheme
])
def test_get_s3_public_url(bucket, key, endpoint, expected_url):
    url = _get_s3_public_url(bucket, key, endpoint)
    assert url == expected_url

# --- Tests for get_s3_dataset_card_presigned_url ---

@patch("hg_localization.s3_utils._get_s3_client")
@patch("hg_localization.s3_utils._get_s3_prefix")
def test_get_s3_dataset_card_presigned_url_s3_not_configured(mock_get_prefix, mock_get_s3_cli, monkeypatch, capsys):
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", None)
    mock_get_s3_cli.return_value = MagicMock() # Client might exist but bucket name is crucial
    url = get_s3_dataset_card_presigned_url("ds_id")
    assert url is None
    captured = capsys.readouterr()
    assert "S3 client not available or bucket not configured" in captured.out

    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", "a-bucket")
    mock_get_s3_cli.return_value = None # Client is None
    url2 = get_s3_dataset_card_presigned_url("ds_id2")
    assert url2 is None
    captured2 = capsys.readouterr()
    assert "S3 client not available or bucket not configured" in captured2.out

@patch("hg_localization.s3_utils._get_s3_client")
@patch("hg_localization.s3_utils._get_s3_prefix")
def test_get_s3_dataset_card_presigned_url_card_not_found_404(mock_get_prefix, mock_get_s3_cli, monkeypatch, capsys):
    mock_client = MagicMock()
    mock_get_s3_cli.return_value = mock_client
    bucket = "presign-bucket"
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", bucket)
    
    dataset_id, cfg, rev = "ds_presign_404", "c1", "r1"
    expected_s3_prefix = f"data_prefix/{dataset_id}/{cfg}/{rev}" # Example, adjust if needed
    mock_get_prefix.return_value = expected_s3_prefix
    expected_card_key = f"{expected_s3_prefix}/dataset_card.md"

    error_404 = ClientError({'Error': {'Code': '404'}}, 'HeadObject')
    mock_client.head_object.side_effect = error_404

    url = get_s3_dataset_card_presigned_url(dataset_id, cfg, rev)
    assert url is None
    mock_get_prefix.assert_called_once_with(dataset_id, cfg, rev)
    mock_client.head_object.assert_called_once_with(Bucket=bucket, Key=expected_card_key)
    mock_client.generate_presigned_url.assert_not_called()
    captured = capsys.readouterr()
    assert f"Cannot generate presigned URL: Dataset card not found on S3 at {expected_card_key}" in captured.out

@patch("hg_localization.s3_utils._get_s3_client")
@patch("hg_localization.s3_utils._get_s3_prefix")
def test_get_s3_dataset_card_presigned_url_head_object_other_client_error(mock_get_prefix, mock_get_s3_cli, monkeypatch, capsys):
    mock_client = MagicMock()
    mock_get_s3_cli.return_value = mock_client
    bucket = "presign-head-err-bucket"
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", bucket)
    expected_s3_prefix = "prefix/for/card_head_err"
    mock_get_prefix.return_value = expected_s3_prefix
    expected_card_key = f"{expected_s3_prefix}/dataset_card.md"

    error_other = ClientError({'Error': {'Code': 'SomeError'}}, 'HeadObject')
    mock_client.head_object.side_effect = error_other

    url = get_s3_dataset_card_presigned_url("ds_head_err")
    assert url is None
    mock_client.head_object.assert_called_once_with(Bucket=bucket, Key=expected_card_key)
    captured = capsys.readouterr()
    assert f"S3 ClientError when checking/generating presigned URL for {expected_card_key}: An error occurred (SomeError)" in captured.out

@patch("hg_localization.s3_utils._get_s3_client")
@patch("hg_localization.s3_utils._get_s3_prefix")
def test_get_s3_dataset_card_presigned_url_generate_url_exception(mock_get_prefix, mock_get_s3_cli, monkeypatch, capsys):
    mock_client = MagicMock()
    mock_get_s3_cli.return_value = mock_client
    bucket = "presign-gen-err-bucket"
    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", bucket)
    expected_s3_prefix = "prefix/for/card_gen_err"
    mock_get_prefix.return_value = expected_s3_prefix
    expected_card_key = f"{expected_s3_prefix}/dataset_card.md"

    mock_client.head_object.return_value = {} # Card exists
    mock_client.generate_presigned_url.side_effect = Exception("Presign generation failed")

    url = get_s3_dataset_card_presigned_url("ds_gen_err")
    assert url is None
    mock_client.generate_presigned_url.assert_called_once_with(
        'get_object',
        Params={'Bucket': bucket, 'Key': expected_card_key},
        ExpiresIn=3600
    )
    captured = capsys.readouterr()
    assert f"Unexpected error generating presigned URL for {expected_card_key}: Presign generation failed" in captured.out

@patch("hg_localization.s3_utils._get_s3_client")
@patch("hg_localization.s3_utils._get_s3_prefix")
def test_get_s3_dataset_card_presigned_url_success(mock_get_prefix, mock_get_s3_cli, monkeypatch, capsys):
    mock_client = MagicMock()
    mock_get_s3_cli.return_value = mock_client
    bucket = "presign-success-bucket"
    dataset_id, cfg, rev = "ds_presign_ok", "my_config", None # Test with None revision
    expires = 1800

    monkeypatch.setattr("hg_localization.s3_utils.S3_BUCKET_NAME", bucket)
    # Patch default names from config as _get_s3_prefix uses them
    monkeypatch.setattr("hg_localization.config.DEFAULT_REVISION_NAME", "default_revision_for_test")
    
    # Ensure DEFAULT_REVISION_NAME from the monkeypatch is used in the expected prefix
    # Retrieve the patched value directly from the config module where it was set
    expected_s3_prefix = f"data_prefix_ok/{dataset_id}/{cfg}/{DEFAULT_REVISION_NAME}" 
    mock_get_prefix.return_value = expected_s3_prefix
    expected_card_key = f"{expected_s3_prefix}/dataset_card.md"
    presigned_url_val = f"https://{bucket}.s3.amazonaws.com/{expected_card_key}?signature=blah"

    mock_client.head_object.return_value = {} # Card found by head_object
    mock_client.generate_presigned_url.return_value = presigned_url_val

    url = get_s3_dataset_card_presigned_url(dataset_id, cfg, rev, expires_in=expires)
    assert url == presigned_url_val

    mock_get_prefix.assert_called_once_with(dataset_id, cfg, rev)
    mock_client.head_object.assert_called_once_with(Bucket=bucket, Key=expected_card_key)
    mock_client.generate_presigned_url.assert_called_once_with(
        'get_object',
        Params={'Bucket': bucket, 'Key': expected_card_key},
        ExpiresIn=expires
    )
    captured = capsys.readouterr()
    assert f"Generated presigned URL for dataset card {expected_card_key}: {presigned_url_val}" in captured.out 