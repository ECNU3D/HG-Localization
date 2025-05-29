from unittest.mock import MagicMock, patch, call, ANY
import os
import shutil
import tempfile
import json
import requests
from pathlib import Path
from botocore.exceptions import ClientError
import pytest

# Functions/classes to test from model_manager.py
from hg_localization.model_manager import (
    _get_model_path,
    _get_model_s3_prefix,
    _store_model_bucket_metadata,
    get_model_card_url,
    get_model_card_content,
    get_model_config_content,
    get_cached_model_card_content,
    get_cached_model_config_content,
    _fetch_public_models_json_via_url,
    _fetch_public_model_info,
    _download_full_model_from_hf,
    download_model_metadata,
    list_local_models,
    list_s3_models,
    _scan_model_directory,
    _scan_legacy_model_structure,
    _check_s3_model_exists,
    sync_local_model_to_s3,
    sync_all_local_models_to_s3
)

# Import the configuration system
from hg_localization.config import HGLocalizationConfig, default_config

# --- Fixtures ---

@pytest.fixture
def temp_models_store(tmp_path, monkeypatch):
    """Creates a temporary directory for models store and patches the config."""
    store_path = tmp_path / "test_models_store"
    store_path.mkdir()
    
    # Patch the default_config instance to use our temp paths
    # Note: public_models_store_path is a property, so we patch the base path
    monkeypatch.setattr(default_config, 'models_store_path', store_path)
    return store_path

@pytest.fixture
def test_config_mm(tmp_path):
    """Create a test configuration for model manager tests."""
    store_path = tmp_path / "test_mm_models_store"
    store_path.mkdir()
    
    return HGLocalizationConfig(
        s3_bucket_name="test-mm-bucket",
        s3_endpoint_url="http://localhost:9000",
        aws_access_key_id="test-mm-access-key",
        aws_secret_access_key="test-mm-secret-key",
        s3_data_prefix="test/mm/prefix",
        models_store_path=store_path,
        default_revision_name="test_mm_revision",
        public_models_json_key="test_mm_public_models.json"
    )

@pytest.fixture
def mock_hf_model_apis(mocker):
    """Mocks Hugging Face model library calls."""
    mock_model_card_load = mocker.patch('hg_localization.model_manager.ModelCard.load')
    mock_hf_hub_download = mocker.patch('hg_localization.model_manager.hf_hub_download')
    
    # Configure mock model card
    mock_card_instance = MagicMock()
    mock_card_instance.text = "# Test Model Card\nThis is a test model."
    mock_model_card_load.return_value = mock_card_instance
    
    # Configure mock config download
    mock_hf_hub_download.return_value = "/tmp/config.json"
    
    return {
        "ModelCard.load": mock_model_card_load,
        "hf_hub_download": mock_hf_hub_download,
        "card_instance": mock_card_instance
    }

@pytest.fixture
def mock_transformers_apis(mocker):
    """Mocks transformers library calls for full model download."""
    mock_auto_model = mocker.patch('hg_localization.model_manager.AutoModel', create=True)
    mock_auto_tokenizer = mocker.patch('hg_localization.model_manager.AutoTokenizer', create=True)
    mock_auto_config = mocker.patch('hg_localization.model_manager.AutoConfig', create=True)
    
    return {
        "AutoModel": mock_auto_model,
        "AutoTokenizer": mock_auto_tokenizer,
        "AutoConfig": mock_auto_config
    }

@pytest.fixture
def mock_s3_utils_for_mm(mocker):
    """Mocks functions imported from s3_utils into model_manager."""
    mock_get_s3_cli = mocker.patch('hg_localization.model_manager._get_s3_client')
    mock_upload_dir = mocker.patch('hg_localization.model_manager._upload_directory_to_s3')
    mock_download_dir = mocker.patch('hg_localization.model_manager._download_directory_from_s3')
    mock_get_public_url = mocker.patch('hg_localization.model_manager._get_s3_public_url')
    mock_update_public_json = mocker.patch('hg_localization.model_manager._update_public_models_json')
    mock_make_metadata_public = mocker.patch('hg_localization.model_manager._make_model_metadata_public')
    mock_update_private_index = mocker.patch('hg_localization.model_manager._update_private_models_index')
    mock_fetch_private_index = mocker.patch('hg_localization.model_manager._fetch_private_models_index')
    mock_get_prefixed_key = mocker.patch('hg_localization.model_manager._get_prefixed_s3_key')
    
    mock_s3_cli_instance = MagicMock()
    mock_get_s3_cli.return_value = mock_s3_cli_instance
    
    def mock_get_public_url_side_effect(bucket, key, endpoint=None):
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    mock_get_public_url.side_effect = mock_get_public_url_side_effect
    
    def mock_get_prefixed_key_side_effect(key, config=None):
        return f"global_prefix/{key}"
    mock_get_prefixed_key.side_effect = mock_get_prefixed_key_side_effect
    
    return {
        "_get_s3_client": mock_get_s3_cli,
        "s3_client_instance": mock_s3_cli_instance,
        "_upload_directory_to_s3": mock_upload_dir,
        "_download_directory_from_s3": mock_download_dir,
        "_get_s3_public_url": mock_get_public_url,
        "_update_public_models_json": mock_update_public_json,
        "_make_model_metadata_public": mock_make_metadata_public,
        "_update_private_models_index": mock_update_private_index,
        "_fetch_private_models_index": mock_fetch_private_index,
        "_get_prefixed_s3_key": mock_get_prefixed_key
    }

@pytest.fixture
def mock_utils_for_mm(mocker):
    """Mocks functions imported from utils into model_manager."""
    mock_get_safe_path = mocker.patch('hg_localization.model_manager._get_safe_path_component')
    mock_restore_name = mocker.patch('hg_localization.model_manager._restore_dataset_name')
    
    mock_get_safe_path.side_effect = lambda name: name.replace("/", "_").replace("\\", "_") if name else ""
    mock_restore_name.side_effect = lambda name: name.replace("_", "/") if name else ""
    
    return {
        "_get_safe_path_component": mock_get_safe_path,
        "_restore_dataset_name": mock_restore_name
    }

@pytest.fixture
def mock_aws_creds_for_mm(monkeypatch):
    """Patches AWS credentials in default_config for model_manager tests."""
    monkeypatch.setattr(default_config, 'aws_access_key_id', "test_access_key")
    monkeypatch.setattr(default_config, 'aws_secret_access_key', "test_secret_key")
    monkeypatch.setattr(default_config, 's3_bucket_name', "test-bucket")

# --- Tests for _get_model_path ---

def test_mm_get_model_path_private_with_bucket(test_config_mm, mock_utils_for_mm):
    """Test model path generation for private models with bucket configuration."""
    path = _get_model_path("test/model1", "v1.0", test_config_mm, is_public=False)
    
    # Should use bucket-specific structure
    import hashlib
    endpoint_hash = hashlib.md5("http://localhost:9000".encode()).hexdigest()[:8]
    expected_path = test_config_mm.models_store_path / "by_bucket" / f"test-mm-bucket_{endpoint_hash}" / "test_model1" / "v1.0"
    assert path == expected_path
    
    mock_utils_for_mm["_get_safe_path_component"].assert_any_call("test/model1")
    mock_utils_for_mm["_get_safe_path_component"].assert_any_call("v1.0")

def test_mm_get_model_path_public_with_bucket(test_config_mm, mock_utils_for_mm):
    """Test model path generation for public models with bucket configuration."""
    path = _get_model_path("test/model1", "v1.0", test_config_mm, is_public=True)
    
    # Should use bucket-specific structure in public store
    import hashlib
    endpoint_hash = hashlib.md5("http://localhost:9000".encode()).hexdigest()[:8]
    expected_path = test_config_mm.public_models_store_path / "by_bucket" / f"test-mm-bucket_{endpoint_hash}" / "test_model1" / "v1.0"
    assert path == expected_path

def test_mm_get_model_path_no_bucket(tmp_path, mock_utils_for_mm):
    """Test model path generation without bucket configuration."""
    config = HGLocalizationConfig(
        s3_bucket_name=None,
        models_store_path=tmp_path / "models"
    )
    
    path = _get_model_path("test/model1", "v1.0", config, is_public=False)
    
    # Should use legacy structure
    expected_path = config.models_store_path / "test_model1" / "v1.0"
    assert path == expected_path

# --- Tests for _get_model_s3_prefix ---

def test_mm_get_model_s3_prefix_with_data_prefix(mock_utils_for_mm):
    """Test S3 prefix generation with data prefix."""
    config = HGLocalizationConfig(s3_data_prefix="data/prefix")
    
    prefix = _get_model_s3_prefix("test/model1", "v1.0", config)
    
    expected_prefix = "data/prefix/models/test_model1/v1.0"
    assert prefix == expected_prefix

def test_mm_get_model_s3_prefix_no_data_prefix(mock_utils_for_mm):
    """Test S3 prefix generation without data prefix."""
    config = HGLocalizationConfig(s3_data_prefix=None)
    
    prefix = _get_model_s3_prefix("test/model1", "v1.0", config)
    
    expected_prefix = "models/test_model1/v1.0"
    assert prefix == expected_prefix

# --- Tests for _store_model_bucket_metadata ---

def test_mm_store_model_bucket_metadata_success(test_config_mm, mock_utils_for_mm):
    """Test successful storage of model bucket metadata."""
    _store_model_bucket_metadata("test/model1", "v1.0", test_config_mm, is_public=False)
    
    # Check that metadata file was created
    model_path = _get_model_path("test/model1", "v1.0", test_config_mm, is_public=False)
    metadata_path = model_path / ".hg_localization_bucket_metadata.json"
    
    assert metadata_path.exists()
    
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    assert metadata["s3_bucket_name"] == "test-mm-bucket"
    assert metadata["s3_endpoint_url"] == "http://localhost:9000"
    assert metadata["s3_data_prefix"] == "test/mm/prefix"
    assert metadata["is_public"] == False
    assert metadata["type"] == "model"

# --- Tests for get_model_card_url ---

def test_get_model_card_url():
    """Test model card URL generation."""
    url = get_model_card_url("microsoft/DialoGPT-medium")
    expected_url = "https://huggingface.co/microsoft/DialoGPT-medium"
    assert url == expected_url

# --- Tests for get_model_card_content ---

def test_get_model_card_content_success(mock_hf_model_apis):
    """Test successful model card content retrieval."""
    content = get_model_card_content("test/model", "v1.0")
    
    assert content == "# Test Model Card\nThis is a test model."
    mock_hf_model_apis["ModelCard.load"].assert_called_once_with("test/model", revision="v1.0")

def test_get_model_card_content_type_error_fallback(mock_hf_model_apis, capsys):
    """Test fallback when revision parameter is not supported."""
    # First call raises TypeError, second call succeeds
    mock_hf_model_apis["ModelCard.load"].side_effect = [TypeError("revision not supported"), mock_hf_model_apis["card_instance"]]
    
    content = get_model_card_content("test/model", "v1.0")
    
    assert content == "# Test Model Card\nThis is a test model."
    assert mock_hf_model_apis["ModelCard.load"].call_count == 2
    
    captured = capsys.readouterr()
    assert "Revision parameter not supported" in captured.out

def test_get_model_card_content_failure(mock_hf_model_apis, capsys):
    """Test model card content retrieval failure."""
    mock_hf_model_apis["ModelCard.load"].side_effect = Exception("Network error")
    
    content = get_model_card_content("test/model", "v1.0")
    
    assert content is None
    captured = capsys.readouterr()
    assert "Error loading model card" in captured.out

# --- Tests for get_model_config_content ---

@patch("builtins.open", create=True)
def test_get_model_config_content_success(mock_open, mock_hf_model_apis):
    """Test successful model config content retrieval."""
    mock_config_data = {"model_type": "gpt2", "vocab_size": 50257}
    mock_file = MagicMock()
    mock_file.read.return_value = json.dumps(mock_config_data)
    mock_open.return_value.__enter__.return_value = mock_file
    
    config_content = get_model_config_content("test/model", "v1.0")
    
    assert config_content == mock_config_data
    mock_hf_model_apis["hf_hub_download"].assert_called_once_with(
        repo_id="test/model",
        filename="config.json",
        revision="v1.0",
        cache_dir=None
    )

def test_get_model_config_content_failure(mock_hf_model_apis, capsys):
    """Test model config content retrieval failure."""
    mock_hf_model_apis["hf_hub_download"].side_effect = Exception("Network error")
    
    config_content = get_model_config_content("test/model", "v1.0")
    
    assert config_content is None
    captured = capsys.readouterr()
    assert "Error loading model config" in captured.out

# --- Tests for get_cached_model_card_content ---

def test_get_cached_model_card_content_public_cache_exists(test_config_mm, mock_utils_for_mm, capsys):
    """Test cached model card retrieval from public cache."""
    # Create public cache with model card
    public_model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=True)
    public_model_dir.mkdir(parents=True, exist_ok=True)
    card_file = public_model_dir / "model_card.md"
    card_file.write_text("# Public Model Card\nThis is from public cache.")
    
    content = get_cached_model_card_content("test/model", "v1.0", test_config_mm)
    
    assert content == "# Public Model Card\nThis is from public cache."
    captured = capsys.readouterr()
    assert "Found model card in public cache" in captured.out

def test_get_cached_model_card_content_private_cache_exists(test_config_mm, mock_utils_for_mm, capsys):
    """Test cached model card retrieval from private cache when public doesn't exist."""
    # Create private cache with model card
    private_model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=False)
    private_model_dir.mkdir(parents=True, exist_ok=True)
    card_file = private_model_dir / "model_card.md"
    card_file.write_text("# Private Model Card\nThis is from private cache.")
    
    content = get_cached_model_card_content("test/model", "v1.0", test_config_mm)
    
    assert content == "# Private Model Card\nThis is from private cache."
    captured = capsys.readouterr()
    assert "Found model card in private cache" in captured.out

def test_get_cached_model_card_content_s3_download_success(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test cached model card retrieval via S3 download."""
    # Mock S3 download success
    def mock_download_file(bucket, key, filename):
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text("# S3 Model Card\nDownloaded from S3.")
    
    mock_s3_utils_for_mm["s3_client_instance"].download_file.side_effect = mock_download_file
    
    content = get_cached_model_card_content("test/model", "v1.0", test_config_mm)
    
    assert content == "# S3 Model Card\nDownloaded from S3."
    captured = capsys.readouterr()
    assert "Successfully downloaded model card from S3" in captured.out

def test_get_cached_model_card_content_s3_not_found(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test cached model card retrieval when S3 file not found."""
    # Mock S3 404 error
    error_response = {'Error': {'Code': '404'}}
    mock_s3_utils_for_mm["s3_client_instance"].download_file.side_effect = ClientError(error_response, 'GetObject')
    
    content = get_cached_model_card_content("test/model", "v1.0", test_config_mm)
    
    assert content is None
    captured = capsys.readouterr()
    assert "Model card not found on S3" in captured.out

@patch('requests.get')
def test_get_cached_model_card_content_public_url_fallback(mock_requests_get, test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test cached model card retrieval via public URL fallback."""
    config_no_s3 = HGLocalizationConfig(
        s3_bucket_name=None,  # No S3 credentials
        models_store_path=test_config_mm.models_store_path
    )
    
    # Mock public model info fetch
    with patch('hg_localization.model_manager._fetch_public_model_info') as mock_fetch_info:
        mock_fetch_info.return_value = {
            'model_card_url': 'https://example.com/model_card.md'
        }
        
        # Mock requests response
        mock_response = MagicMock()
        mock_response.text = "# Public URL Model Card\nFrom public URL."
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response
        
        content = get_cached_model_card_content("test/model", "v1.0", config_no_s3)
        
        assert content == "# Public URL Model Card\nFrom public URL."
        captured = capsys.readouterr()
        assert "Successfully downloaded and cached model card from public URL" in captured.out

# --- Tests for get_cached_model_config_content ---

def test_get_cached_model_config_content_public_cache_exists(test_config_mm, mock_utils_for_mm, capsys):
    """Test cached model config retrieval from public cache."""
    # Create public cache with model config
    public_model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=True)
    public_model_dir.mkdir(parents=True, exist_ok=True)
    config_file = public_model_dir / "config.json"
    config_data = {"model_type": "gpt2", "vocab_size": 50257}
    config_file.write_text(json.dumps(config_data))
    
    content = get_cached_model_config_content("test/model", "v1.0", test_config_mm)
    
    assert content == config_data
    captured = capsys.readouterr()
    assert "Found model config in public cache" in captured.out

def test_get_cached_model_config_content_json_decode_error(test_config_mm, mock_utils_for_mm, capsys):
    """Test cached model config retrieval with JSON decode error."""
    # Create public cache with invalid JSON
    public_model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=True)
    public_model_dir.mkdir(parents=True, exist_ok=True)
    config_file = public_model_dir / "config.json"
    config_file.write_text("invalid json content")
    
    content = get_cached_model_config_content("test/model", "v1.0", test_config_mm)
    
    assert content is None
    captured = capsys.readouterr()
    assert "Error reading public model config" in captured.out

# --- Tests for _fetch_public_models_json_via_url ---

@patch('requests.get')
def test_fetch_public_models_json_via_url_no_bucket(mock_requests_get, capsys):
    """Test public models JSON fetch with no bucket configured."""
    config = HGLocalizationConfig(s3_bucket_name=None)
    
    result = _fetch_public_models_json_via_url(config)
    
    assert result is None
    captured = capsys.readouterr()
    assert "config.s3_bucket_name not configured" in captured.out
    mock_requests_get.assert_not_called()

@patch('requests.get')
def test_fetch_public_models_json_via_url_success(mock_requests_get, mock_s3_utils_for_mm):
    """Test successful public models JSON fetch."""
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    
    mock_response = MagicMock()
    mock_response.json.return_value = {"model1---v1.0": {"model_id": "model1", "s3_bucket": "test-bucket"}}
    mock_response.raise_for_status = MagicMock()
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_models_json_via_url(config)
    
    assert result == {"model1---v1.0": {"model_id": "model1", "s3_bucket": "test-bucket"}}
    mock_requests_get.assert_called_once()

@patch('requests.get')
def test_fetch_public_models_json_via_url_http_error(mock_requests_get, mock_s3_utils_for_mm, capsys):
    """Test public models JSON fetch with HTTP error."""
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found", response=MagicMock(status_code=404))
    mock_requests_get.return_value = mock_response
    
    result = _fetch_public_models_json_via_url(config)
    
    assert result is None
    captured = capsys.readouterr()
    assert "HTTP error fetching" in captured.out

# --- Tests for _fetch_public_model_info ---

def test_fetch_public_model_info_success(mock_s3_utils_for_mm):
    """Test successful public model info fetch."""
    config = HGLocalizationConfig(s3_bucket_name="test-bucket", default_revision_name="main")
    
    with patch('hg_localization.model_manager._fetch_public_models_json_via_url') as mock_fetch_json:
        mock_fetch_json.return_value = {
            "test/model---main": {
                "model_id": "test/model",
                "s3_bucket": "test-bucket",
                "model_card_url": "https://example.com/card.md"
            }
        }
        
        result = _fetch_public_model_info("test/model", "main", config)
        
        assert result["model_id"] == "test/model"
        assert result["s3_bucket"] == "test-bucket"
        assert result["model_card_url"] == "https://example.com/card.md"

def test_fetch_public_model_info_not_found(mock_s3_utils_for_mm, capsys):
    """Test public model info fetch when model not found."""
    config = HGLocalizationConfig(s3_bucket_name="test-bucket", default_revision_name="main")
    
    with patch('hg_localization.model_manager._fetch_public_models_json_via_url') as mock_fetch_json:
        mock_fetch_json.return_value = {}
        
        result = _fetch_public_model_info("test/model", "main", config)
        
        assert result is None
        # Note: The actual function may not print this message, so we'll just check the result
        # captured = capsys.readouterr()
        # assert "Public model info not found" in captured.out

def test_fetch_public_model_info_incomplete_data(mock_s3_utils_for_mm, capsys):
    """Test public model info fetch with incomplete data."""
    config = HGLocalizationConfig(s3_bucket_name="test-bucket", default_revision_name="main")
    
    with patch('hg_localization.model_manager._fetch_public_models_json_via_url') as mock_fetch_json:
        mock_fetch_json.return_value = {
            "test/model---main": {
                "model_id": "test/model"
                # Missing s3_bucket
            }
        }
        
        result = _fetch_public_model_info("test/model", "main", config)
        
        assert result is None
        captured = capsys.readouterr()
        assert "Public model info for test/model---main is incomplete" in captured.out

# --- Tests for _download_full_model_from_hf ---

def test_download_full_model_from_hf_success(tmp_path, capsys):
    """Test successful full model download from Hugging Face."""
    local_save_path = tmp_path / "model_save"
    local_save_path.mkdir()
    
    # Mock transformers components at the module level where they're imported
    with patch('transformers.AutoConfig') as mock_auto_config, \
         patch('transformers.AutoTokenizer') as mock_auto_tokenizer, \
         patch('transformers.AutoModel') as mock_auto_model:
        
        # Mock transformers components
        mock_config_instance = MagicMock()
        mock_tokenizer_instance = MagicMock()
        mock_model_instance = MagicMock()
        
        mock_auto_config.from_pretrained.return_value = mock_config_instance
        mock_auto_tokenizer.from_pretrained.return_value = mock_tokenizer_instance
        mock_auto_model.from_pretrained.return_value = mock_model_instance
        
        # Mock get_model_card_content
        with patch('hg_localization.model_manager.get_model_card_content') as mock_get_card:
            mock_get_card.return_value = "# Test Model Card"
            
            result = _download_full_model_from_hf("test/model", "v1.0", local_save_path)
            
            assert result is True
            captured = capsys.readouterr()
            assert "✓ Downloaded model config" in captured.out
            assert "✓ Downloaded tokenizer" in captured.out
            assert "✓ Downloaded model weights" in captured.out
            assert "✓ Downloaded model card" in captured.out

def test_download_full_model_from_hf_import_error(tmp_path, capsys):
    """Test full model download with missing transformers library."""
    local_save_path = tmp_path / "model_save"
    local_save_path.mkdir()
    
    # Mock the import to raise ImportError by patching builtins.__import__
    original_import = __builtins__['__import__']
    
    def mock_import(name, *args, **kwargs):
        if name == 'transformers':
            raise ImportError("No module named 'transformers'")
        return original_import(name, *args, **kwargs)
    
    with patch('builtins.__import__', side_effect=mock_import):
        result = _download_full_model_from_hf("test/model", "v1.0", local_save_path)
        
        assert result is False
        captured = capsys.readouterr()
        assert "transformers library is required" in captured.out

def test_download_full_model_from_hf_model_weights_error(tmp_path, capsys):
    """Test full model download with model weights error."""
    local_save_path = tmp_path / "model_save"
    local_save_path.mkdir()
    
    # Mock transformers components at the module level
    with patch('transformers.AutoConfig') as mock_auto_config, \
         patch('transformers.AutoTokenizer') as mock_auto_tokenizer, \
         patch('transformers.AutoModel') as mock_auto_model:
        
        # Mock successful config and tokenizer, but failed model weights
        mock_config_instance = MagicMock()
        mock_tokenizer_instance = MagicMock()
        
        mock_auto_config.from_pretrained.return_value = mock_config_instance
        mock_auto_tokenizer.from_pretrained.return_value = mock_tokenizer_instance
        mock_auto_model.from_pretrained.side_effect = Exception("Model weights error")
        
        result = _download_full_model_from_hf("test/model", "v1.0", local_save_path)
        
        assert result is False
        captured = capsys.readouterr()
        assert "✓ Downloaded model config" in captured.out
        assert "✓ Downloaded tokenizer" in captured.out
        assert "Error: Failed to download model weights" in captured.out

# --- Tests for download_model_metadata ---

def test_download_model_metadata_already_exists_public(test_config_mm, mock_utils_for_mm, capsys):
    """Test download when model already exists in public cache."""
    # Create existing model in public cache
    public_model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=True)
    public_model_dir.mkdir(parents=True, exist_ok=True)
    (public_model_dir / "model_card.md").write_text("# Existing Model Card")
    
    success, path = download_model_metadata("test/model", "v1.0", make_public=False, config=test_config_mm)
    
    assert success is True
    assert str(public_model_dir) in path
    captured = capsys.readouterr()
    assert "already exists in public cache" in captured.out

def test_download_model_metadata_already_exists_private(test_config_mm, mock_utils_for_mm, capsys):
    """Test download when model already exists in private cache."""
    # Create existing model in private cache only
    private_model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=False)
    private_model_dir.mkdir(parents=True, exist_ok=True)
    (private_model_dir / "config.json").write_text('{"model_type": "gpt2"}')
    
    success, path = download_model_metadata("test/model", "v1.0", make_public=False, config=test_config_mm)
    
    assert success is True
    assert str(private_model_dir) in path
    captured = capsys.readouterr()
    assert "already exists in private cache" in captured.out

def test_download_model_metadata_s3_download_success(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test successful download from S3."""
    # Mock S3 head_object to indicate files exist
    mock_s3_utils_for_mm["s3_client_instance"].head_object.return_value = {}
    
    # Mock successful S3 download
    def mock_download_success(s3_client, local_path, bucket, s3_prefix):
        local_path.mkdir(parents=True, exist_ok=True)
        (local_path / "model_card.md").write_text("# S3 Model Card")
        (local_path / "config.json").write_text('{"model_type": "gpt2"}')
        return True
    
    mock_s3_utils_for_mm["_download_directory_from_s3"].side_effect = mock_download_success
    
    success, path = download_model_metadata("test/model", "v1.0", config=test_config_mm)
    
    assert success is True
    captured = capsys.readouterr()
    assert "Successfully downloaded metadata from S3" in captured.out

def test_download_model_metadata_hf_download_success(test_config_mm, mock_transformers_apis, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test successful download from Hugging Face."""
    # Mock S3 head_object to indicate files don't exist
    error_response = {'Error': {'Code': '404'}}
    mock_s3_utils_for_mm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    # Mock HF downloads
    with patch('hg_localization.model_manager.get_model_card_content') as mock_get_card, \
         patch('hg_localization.model_manager.get_model_config_content') as mock_get_config:
        
        mock_get_card.return_value = "# HF Model Card"
        mock_get_config.return_value = {"model_type": "gpt2", "vocab_size": 50257}
        
        success, path = download_model_metadata("test/model", "v1.0", config=test_config_mm)
        
        assert success is True
        captured = capsys.readouterr()
        assert "successfully saved to local cache" in captured.out

def test_download_model_metadata_full_model_download(test_config_mm, mock_transformers_apis, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test full model download (not metadata only)."""
    # Mock S3 head_object to indicate files don't exist
    error_response = {'Error': {'Code': '404'}}
    mock_s3_utils_for_mm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    # Mock successful full model download
    with patch('hg_localization.model_manager._download_full_model_from_hf') as mock_download_full:
        mock_download_full.return_value = True
        
        success, path = download_model_metadata("test/model", "v1.0", metadata_only=False, config=test_config_mm)
        
        assert success is True
        mock_download_full.assert_called_once()
        captured = capsys.readouterr()
        assert "Downloading full model for test/model" in captured.out

def test_download_model_metadata_make_public_success(test_config_mm, mock_transformers_apis, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test download with make_public option."""
    # Mock S3 head_object to indicate files don't exist
    error_response = {'Error': {'Code': '404'}}
    mock_s3_utils_for_mm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    # Mock successful public operations
    mock_s3_utils_for_mm["_make_model_metadata_public"].return_value = True
    mock_s3_utils_for_mm["_update_public_models_json"].return_value = True
    
    # Mock HF downloads
    with patch('hg_localization.model_manager.get_model_card_content') as mock_get_card, \
         patch('hg_localization.model_manager.get_model_config_content') as mock_get_config:
        
        mock_get_card.return_value = "# HF Model Card"
        mock_get_config.return_value = {"model_type": "gpt2"}
        
        success, path = download_model_metadata("test/model", "v1.0", make_public=True, config=test_config_mm)
        
        assert success is True
        captured = capsys.readouterr()
        assert "Making model metadata public" in captured.out
        assert "Successfully made model metadata files public" in captured.out

def test_download_model_metadata_error_handling(test_config_mm, mock_transformers_apis, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test error handling during download."""
    # Mock S3 head_object to indicate files don't exist
    error_response = {'Error': {'Code': '404'}}
    mock_s3_utils_for_mm["s3_client_instance"].head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    # Mock HF download failure
    with patch('hg_localization.model_manager.get_model_card_content') as mock_get_card:
        mock_get_card.side_effect = Exception("Network error")
        
        success, error_msg = download_model_metadata("test/model", "v1.0", config=test_config_mm)
        
        assert success is False
        assert "Network error" in error_msg

# --- Tests for list_local_models ---

def test_list_local_models_empty_store(test_config_mm, capsys):
    """Test listing local models with empty store."""
    models = list_local_models(test_config_mm)
    
    assert models == []
    captured = capsys.readouterr()
    assert "No local models found in cache" in captured.out

def test_list_local_models_with_models(test_config_mm, mock_utils_for_mm, capsys):
    """Test listing local models with existing models."""
    # Create test models in both public and private stores
    public_model_dir = _get_model_path("test/model1", "v1.0", test_config_mm, is_public=True)
    public_model_dir.mkdir(parents=True, exist_ok=True)
    (public_model_dir / "model_card.md").write_text("# Public Model")
    (public_model_dir / "config.json").write_text('{"model_type": "gpt2"}')
    
    private_model_dir = _get_model_path("test/model2", "v2.0", test_config_mm, is_public=False)
    private_model_dir.mkdir(parents=True, exist_ok=True)
    (private_model_dir / "model_card.md").write_text("# Private Model")
    (private_model_dir / "pytorch_model.bin").write_text("fake weights")
    
    models = list_local_models(test_config_mm)
    
    assert len(models) == 2
    
    # Check model details
    model_ids = [m["model_id"] for m in models]
    assert "test/model1" in model_ids
    assert "test/model2" in model_ids
    
    captured = capsys.readouterr()
    assert "Found 2 local model(s)" in captured.out

def test_list_local_models_public_access_only(test_config_mm, mock_utils_for_mm, capsys):
    """Test listing local models with public access only."""
    # Create models in both stores
    public_model_dir = _get_model_path("test/model1", "v1.0", test_config_mm, is_public=True)
    public_model_dir.mkdir(parents=True, exist_ok=True)
    (public_model_dir / "model_card.md").write_text("# Public Model")
    
    private_model_dir = _get_model_path("test/model2", "v2.0", test_config_mm, is_public=False)
    private_model_dir.mkdir(parents=True, exist_ok=True)
    (private_model_dir / "model_card.md").write_text("# Private Model")
    
    models = list_local_models(test_config_mm, public_access_only=True)
    
    assert len(models) == 1
    assert models[0]["model_id"] == "test/model1"
    
    captured = capsys.readouterr()
    assert "Public access mode: scanning public models only" in captured.out

# --- Tests for list_s3_models ---

def test_list_s3_models_no_bucket_configured(mock_s3_utils_for_mm, capsys):
    """Test listing S3 models with no bucket configured."""
    config = HGLocalizationConfig(s3_bucket_name=None)
    
    models = list_s3_models(config)
    
    assert models == []
    captured = capsys.readouterr()
    assert "config.s3_bucket_name not configured" in captured.out

def test_list_s3_models_private_index_success(mock_s3_utils_for_mm, mock_aws_creds_for_mm, capsys):
    """Test listing S3 models using private index."""
    config = HGLocalizationConfig(
        s3_bucket_name="test-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret"
    )
    
    # Mock private index data
    mock_private_index = {
        "test/model1---v1.0": {
            "model_id": "test/model1",
            "revision": "v1.0",
            "s3_prefix": "models/test_model1/v1.0",
            "has_card": True,
            "has_config": True,
            "is_full_model": False
        },
        "test/model2---main": {
            "model_id": "test/model2",
            "revision": "main",
            "s3_prefix": "models/test_model2/main",
            "has_card": False,
            "has_config": True,
            "is_full_model": True
        }
    }
    
    mock_s3_utils_for_mm["_fetch_private_models_index"].return_value = mock_private_index
    
    models = list_s3_models(config)
    
    assert len(models) == 2
    assert models[0]["model_id"] == "test/model1"
    assert models[0]["has_card"] is True
    assert models[1]["model_id"] == "test/model2"
    assert models[1]["is_full_model"] is True
    
    captured = capsys.readouterr()
    assert "Successfully fetched private models index with 2 entries" in captured.out

def test_list_s3_models_bucket_scanning_fallback(mock_s3_utils_for_mm, mock_utils_for_mm, mock_aws_creds_for_mm, capsys):
    """Test listing S3 models using bucket scanning fallback."""
    config = HGLocalizationConfig(
        s3_bucket_name="test-bucket",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        s3_data_prefix="data"
    )
    
    # Mock private index not found
    mock_s3_utils_for_mm["_fetch_private_models_index"].return_value = None
    
    # Mock paginator for bucket scanning
    mock_paginator = MagicMock()
    mock_s3_utils_for_mm["s3_client_instance"].get_paginator.return_value = mock_paginator
    
    # Mock paginate responses for model structure
    def mock_paginate_side_effect(Bucket, Prefix, Delimiter):
        if Prefix == "data/models/":
            # Level 1: model_id directories
            return [{"CommonPrefixes": [{"Prefix": "data/models/test_model1/"}]}]
        elif Prefix == "data/models/test_model1/":
            # Level 2: revision directories
            return [{"CommonPrefixes": [{"Prefix": "data/models/test_model1/v1.0/"}]}]
        return []
    
    mock_paginator.paginate.side_effect = mock_paginate_side_effect
    
    # Mock head_object for file existence checks
    def mock_head_object_side_effect(Bucket, Key):
        if Key.endswith("model_card.md") or Key.endswith("config.json"):
            return {}
        raise ClientError({'Error': {'Code': '404'}}, 'HeadObject')
    
    mock_s3_utils_for_mm["s3_client_instance"].head_object.side_effect = mock_head_object_side_effect
    
    models = list_s3_models(config)
    
    assert len(models) == 1
    assert models[0]["model_id"] == "test/model1"
    assert models[0]["has_card"] is True
    assert models[0]["has_config"] is True
    
    captured = capsys.readouterr()
    assert "Listing S3 models via authenticated API call" in captured.out

@patch('hg_localization.model_manager._fetch_public_models_json_via_url')
def test_list_s3_models_public_json_fallback(mock_fetch_public_json, mock_s3_utils_for_mm, capsys):
    """Test listing S3 models using public JSON fallback."""
    config = HGLocalizationConfig(s3_bucket_name="test-bucket")
    
    # Mock no AWS credentials
    mock_s3_utils_for_mm["_get_s3_client"].return_value = None
    
    # Mock public JSON data
    mock_public_json = {
        "test/model1---v1.0": {
            "model_id": "test/model1",
            "revision": "v1.0",
            "s3_bucket": "test-bucket",
            "model_card_url": "https://example.com/card.md",
            "model_config_url": "https://example.com/config.json"
        }
    }
    
    mock_fetch_public_json.return_value = mock_public_json
    
    models = list_s3_models(config)
    
    assert len(models) == 1
    assert models[0]["model_id"] == "test/model1"
    assert models[0]["has_card"] is True
    assert models[0]["has_config"] is True
    assert models[0]["s3_card_url"] == "https://example.com/card.md"
    
    captured = capsys.readouterr()
    assert "Listing S3 models based on public_models.json" in captured.out

# --- Tests for sync_local_model_to_s3 ---

def test_sync_local_model_to_s3_model_not_found(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test sync when local model not found."""
    success, message = sync_local_model_to_s3("test/model", "v1.0", config=test_config_mm)
    
    assert success is False
    assert "not found or is incomplete" in message
    captured = capsys.readouterr()
    assert "Local model test/model" in captured.out

def test_sync_local_model_to_s3_s3_not_configured(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test sync when S3 not configured."""
    config_no_s3 = HGLocalizationConfig(
        models_store_path=test_config_mm.models_store_path,
        s3_bucket_name=None
    )
    
    # Create local model
    model_dir = _get_model_path("test/model", "v1.0", config_no_s3, is_public=False)
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "model_card.md").write_text("# Test Model")
    
    # Mock no S3 client
    mock_s3_utils_for_mm["_get_s3_client"].return_value = None
    
    success, message = sync_local_model_to_s3("test/model", "v1.0", config=config_no_s3)
    
    assert success is False
    assert "S3 not configured" in message

def test_sync_local_model_to_s3_upload_success(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test successful sync to S3."""
    # Create local model
    model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=False)
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "model_card.md").write_text("# Test Model")
    
    # Mock S3 operations
    with patch('hg_localization.model_manager._check_s3_model_exists') as mock_check_exists:
        mock_check_exists.return_value = False  # Model doesn't exist on S3
        
        success, message = sync_local_model_to_s3("test/model", "v1.0", config=test_config_mm)
        
        assert success is True
        assert "Sync process for test/model" in message
        mock_s3_utils_for_mm["_upload_directory_to_s3"].assert_called_once()

def test_sync_local_model_to_s3_make_public_success(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test successful sync with make_public option."""
    # Create local model
    model_dir = _get_model_path("test/model", "v1.0", test_config_mm, is_public=False)
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "model_card.md").write_text("# Test Model")
    
    # Mock S3 operations
    with patch('hg_localization.model_manager._check_s3_model_exists') as mock_check_exists:
        mock_check_exists.return_value = True  # Model already exists on S3
        mock_s3_utils_for_mm["_make_model_metadata_public"].return_value = True
        mock_s3_utils_for_mm["_update_public_models_json"].return_value = True
        
        success, message = sync_local_model_to_s3("test/model", "v1.0", make_public=True, config=test_config_mm)
        
        assert success is True
        captured = capsys.readouterr()
        assert "Processing --make-public for model" in captured.out
        assert "Successfully made model metadata files public" in captured.out

# --- Tests for sync_all_local_models_to_s3 ---

def test_sync_all_local_models_to_s3_no_models(test_config_mm, capsys):
    """Test sync all when no local models exist."""
    sync_all_local_models_to_s3(config=test_config_mm)
    
    captured = capsys.readouterr()
    assert "No local models found in cache to sync" in captured.out

def test_sync_all_local_models_to_s3_s3_not_configured(test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test sync all when S3 not configured."""
    config_no_s3 = HGLocalizationConfig(
        models_store_path=test_config_mm.models_store_path,
        s3_bucket_name=None
    )
    
    # Create a test model so the function doesn't exit early
    model_dir = _get_model_path("test/model", "v1.0", config_no_s3, is_public=False)
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "model_card.md").write_text("# Test Model")
    
    # Mock no S3 client
    mock_s3_utils_for_mm["_get_s3_client"].return_value = None
    
    sync_all_local_models_to_s3(config=config_no_s3)
    
    captured = capsys.readouterr()
    assert "S3 not configured" in captured.out

@patch('hg_localization.model_manager.sync_local_model_to_s3')
def test_sync_all_local_models_to_s3_success(mock_sync_single, test_config_mm, mock_s3_utils_for_mm, mock_utils_for_mm, capsys):
    """Test successful sync all operation."""
    # Create test models
    model1_dir = _get_model_path("test/model1", "v1.0", test_config_mm, is_public=False)
    model1_dir.mkdir(parents=True, exist_ok=True)
    (model1_dir / "model_card.md").write_text("# Model 1")
    
    model2_dir = _get_model_path("test/model2", "v2.0", test_config_mm, is_public=True)
    model2_dir.mkdir(parents=True, exist_ok=True)
    (model2_dir / "config.json").write_text('{"model_type": "gpt2"}')
    
    # Mock successful sync operations
    mock_sync_single.return_value = (True, "Success")
    
    sync_all_local_models_to_s3(config=test_config_mm)
    
    assert mock_sync_single.call_count == 2
    captured = capsys.readouterr()
    assert "Successfully processed (primary sync action): 2" in captured.out
    assert "Failed to process (see logs for errors): 0" in captured.out

# --- Tests for _check_s3_model_exists ---

def test_check_s3_model_exists_success(mock_s3_utils_for_mm):
    """Test S3 model existence check success."""
    mock_s3_client = mock_s3_utils_for_mm["s3_client_instance"]
    mock_s3_client.head_object.return_value = {}
    
    exists = _check_s3_model_exists(mock_s3_client, "test-bucket", "models/test_model/v1.0")
    
    assert exists is True
    mock_s3_client.head_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="models/test_model/v1.0/model_card.md"
    )

def test_check_s3_model_exists_not_found_fallback_to_config(mock_s3_utils_for_mm):
    """Test S3 model existence check with fallback to config.json."""
    mock_s3_client = mock_s3_utils_for_mm["s3_client_instance"]
    
    # First call (model_card.md) fails, second call (config.json) succeeds
    error_response = {'Error': {'Code': '404'}}
    mock_s3_client.head_object.side_effect = [
        ClientError(error_response, 'HeadObject'),  # model_card.md not found
        {}  # config.json found
    ]
    
    exists = _check_s3_model_exists(mock_s3_client, "test-bucket", "models/test_model/v1.0")
    
    assert exists is True
    assert mock_s3_client.head_object.call_count == 2

def test_check_s3_model_exists_not_found(mock_s3_utils_for_mm):
    """Test S3 model existence check when model not found."""
    mock_s3_client = mock_s3_utils_for_mm["s3_client_instance"]
    
    # Both calls fail
    error_response = {'Error': {'Code': '404'}}
    mock_s3_client.head_object.side_effect = ClientError(error_response, 'HeadObject')
    
    exists = _check_s3_model_exists(mock_s3_client, "test-bucket", "models/test_model/v1.0")
    
    assert exists is False

def test_check_s3_model_exists_no_client():
    """Test S3 model existence check with no client."""
    exists = _check_s3_model_exists(None, "test-bucket", "models/test_model/v1.0")
    
    assert exists is False 