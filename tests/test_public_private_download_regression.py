"""
Test cases for public/private cache download regression prevention.

This module tests the specific fix for the issue where downloading a public dataset
would return early if a private version already existed, instead of downloading
to the public cache as requested.
"""

import pytest
from unittest.mock import MagicMock, patch, call
import tempfile
import json
from pathlib import Path
from datasets import DatasetDict

from hg_localization.dataset_manager import download_dataset, _get_dataset_path
from hg_localization.config import HGLocalizationConfig


@pytest.fixture
def test_config_with_public_private(tmp_path):
    """Create a test configuration with both public and private paths."""
    store_path = tmp_path / "test_datasets_store"
    store_path.mkdir()
    
    return HGLocalizationConfig(
        s3_bucket_name="test-bucket",
        s3_endpoint_url="http://localhost:9000",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        s3_data_prefix="test/prefix",
        datasets_store_path=store_path,
        default_config_name="default",
        default_revision_name="main",
        public_datasets_json_key="public_datasets.json",
        public_datasets_zip_dir_prefix="public_datasets_zip"
    )


@pytest.fixture
def mock_all_dependencies(mocker):
    """Mock all external dependencies for download_dataset tests."""
    # Mock HF datasets
    mock_load_dataset = mocker.patch('hg_localization.dataset_manager.load_dataset')
    mock_dataset_instance = MagicMock(spec=DatasetDict)
    mock_dataset_instance.save_to_disk = MagicMock()
    mock_load_dataset.return_value = mock_dataset_instance
    
    # Mock S3 utils
    mock_get_s3_client = mocker.patch('hg_localization.dataset_manager._get_s3_client')
    mock_s3_client = MagicMock()
    mock_get_s3_client.return_value = mock_s3_client
    
    mock_get_s3_prefix = mocker.patch('hg_localization.dataset_manager._get_s3_prefix')
    mock_get_s3_prefix.return_value = "test/prefix/dataset"
    
    mock_check_s3_exists = mocker.patch('hg_localization.dataset_manager._check_s3_dataset_exists')
    mock_check_s3_exists.return_value = False  # Default: not found on S3
    
    mock_download_from_s3 = mocker.patch('hg_localization.dataset_manager._download_directory_from_s3')
    mock_upload_to_s3 = mocker.patch('hg_localization.dataset_manager._upload_directory_to_s3')
    
    # Mock public dataset fetching
    mock_fetch_public_info = mocker.patch('hg_localization.dataset_manager._fetch_public_dataset_info')
    mock_fetch_public_info.return_value = None  # Default: no public info
    
    # Mock dataset card
    mock_get_card = mocker.patch('hg_localization.dataset_manager.get_dataset_card_content')
    mock_get_card.return_value = "# Test Dataset Card"
    
    # Mock utils
    mock_get_safe_path = mocker.patch('hg_localization.dataset_manager._get_safe_path_component')
    mock_get_safe_path.side_effect = lambda x: x.replace("/", "_") if x else ""
    
    # Mock private index update
    mock_update_private_index = mocker.patch('hg_localization.dataset_manager._update_private_datasets_index')
    mock_update_private_index.return_value = True
    
    return {
        "load_dataset": mock_load_dataset,
        "dataset_instance": mock_dataset_instance,
        "s3_client": mock_s3_client,
        "get_s3_client": mock_get_s3_client,
        "get_s3_prefix": mock_get_s3_prefix,
        "check_s3_exists": mock_check_s3_exists,
        "download_from_s3": mock_download_from_s3,
        "upload_to_s3": mock_upload_to_s3,
        "fetch_public_info": mock_fetch_public_info,
        "get_card": mock_get_card,
        "get_safe_path": mock_get_safe_path,
        "update_private_index": mock_update_private_index
    }


def create_mock_dataset(path: Path, is_public: bool = False):
    """Helper to create a mock dataset directory structure."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "dataset_info.json").write_text(json.dumps({"is_public": is_public}))
    (path / "dataset_card.md").write_text(f"# {'Public' if is_public else 'Private'} Dataset")


class TestPublicPrivateCacheRegression:
    """Test cases for public/private cache download regression."""
    
    def test_force_public_cache_with_existing_private_downloads_to_public(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that force_public_cache=True downloads to public cache even when private exists.
        
        This is the main regression test for the issue where the system would return
        the private version instead of downloading to public cache.
        """
        dataset_id = "test/dataset"
        config = test_config_with_public_private
        
        # Create existing private dataset
        private_path = _get_dataset_path(dataset_id, config=config, is_public=False)
        create_mock_dataset(private_path, is_public=False)
        
        # Ensure public path doesn't exist
        public_path = _get_dataset_path(dataset_id, config=config, is_public=True)
        assert not public_path.exists(), "Public path should not exist initially"
        
        # Call download_dataset with force_public_cache=True
        success, result_path = download_dataset(
            dataset_id=dataset_id,
            force_public_cache=True,
            config=config
        )
        
        # Verify success and that it downloaded to public path
        assert success, "Download should succeed"
        assert str(public_path) == result_path, f"Should return public path, got {result_path}"
        
        # Verify HF download was called (since we're forcing public cache)
        mock_all_dependencies["load_dataset"].assert_called_once_with(
            path=dataset_id, name=None, revision=None, trust_remote_code=False
        )
        
        # Verify save_to_disk was called with public path
        mock_all_dependencies["dataset_instance"].save_to_disk.assert_called_once_with(str(public_path))
        
        captured = capsys.readouterr()
        assert "Downloading dataset" in captured.out
        assert "from Hugging Face" in captured.out
    
    def test_make_public_with_existing_private_downloads_to_public(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that make_public=True downloads to public cache even when private exists.
        """
        dataset_id = "test/dataset"
        config = test_config_with_public_private
        
        # Create existing private dataset
        private_path = _get_dataset_path(dataset_id, config=config, is_public=False)
        create_mock_dataset(private_path, is_public=False)
        
        # Ensure public path doesn't exist
        public_path = _get_dataset_path(dataset_id, config=config, is_public=True)
        assert not public_path.exists(), "Public path should not exist initially"
        
        # Call download_dataset with make_public=True
        success, result_path = download_dataset(
            dataset_id=dataset_id,
            make_public=True,
            config=config
        )
        
        # Verify success and that it downloaded to public path
        assert success, "Download should succeed"
        assert str(public_path) == result_path, f"Should return public path, got {result_path}"
        
        # Verify HF download was called
        mock_all_dependencies["load_dataset"].assert_called_once()
        
        # Verify save_to_disk was called with public path
        mock_all_dependencies["dataset_instance"].save_to_disk.assert_called_once_with(str(public_path))
    
    def test_private_download_prefers_existing_public_over_private(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that private download (default) prefers existing public version over private.
        """
        dataset_id = "test/dataset"
        config = test_config_with_public_private
        
        # Create both public and private datasets
        private_path = _get_dataset_path(dataset_id, config=config, is_public=False)
        public_path = _get_dataset_path(dataset_id, config=config, is_public=True)
        
        create_mock_dataset(private_path, is_public=False)
        create_mock_dataset(public_path, is_public=True)
        
        # Call download_dataset without force_public_cache or make_public
        success, result_path = download_dataset(
            dataset_id=dataset_id,
            config=config
        )
        
        # Should return public path (preferred over private)
        assert success, "Download should succeed"
        assert str(public_path) == result_path, f"Should prefer public path, got {result_path}"
        
        # Should not call HF download since dataset exists locally
        mock_all_dependencies["load_dataset"].assert_not_called()
        
        captured = capsys.readouterr()
        assert "already exists in public cache" in captured.out
    
    def test_private_download_uses_private_when_no_public_exists(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that private download uses private version when no public version exists.
        """
        dataset_id = "test/dataset"
        config = test_config_with_public_private
        
        # Create only private dataset
        private_path = _get_dataset_path(dataset_id, config=config, is_public=False)
        create_mock_dataset(private_path, is_public=False)
        
        # Ensure public path doesn't exist
        public_path = _get_dataset_path(dataset_id, config=config, is_public=True)
        assert not public_path.exists(), "Public path should not exist"
        
        # Call download_dataset without force_public_cache or make_public
        success, result_path = download_dataset(
            dataset_id=dataset_id,
            config=config
        )
        
        # Should return private path
        assert success, "Download should succeed"
        assert str(private_path) == result_path, f"Should return private path, got {result_path}"
        
        # Should not call HF download since dataset exists locally
        mock_all_dependencies["load_dataset"].assert_not_called()
        
        captured = capsys.readouterr()
        assert "already exists in private cache" in captured.out
    
    def test_public_cache_checks_public_datasets_even_with_private_existing(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that when saving to public cache, system checks for public datasets even if private exists.
        """
        dataset_id = "test/dataset"
        config = test_config_with_public_private
        
        # Create existing private dataset
        private_path = _get_dataset_path(dataset_id, config=config, is_public=False)
        create_mock_dataset(private_path, is_public=False)
        
        # Mock public dataset info to simulate finding a public dataset
        mock_all_dependencies["fetch_public_info"].return_value = {
            "s3_zip_key": "public_datasets_zip/test_dataset---default---main.zip",
            "s3_bucket": "test-bucket"
        }
        
        # Mock requests for public dataset download
        with patch('hg_localization.dataset_manager.requests.get') as mock_requests, \
             patch('hg_localization.dataset_manager._unzip_file') as mock_unzip, \
             patch('hg_localization.dataset_manager._get_prefixed_s3_key') as mock_get_prefixed_key, \
             patch('hg_localization.dataset_manager._get_s3_public_url') as mock_get_public_url:
            
            mock_get_prefixed_key.return_value = "test/prefix/public_datasets_zip/test_dataset---default---main.zip"
            mock_get_public_url.return_value = "https://test-bucket.s3.amazonaws.com/test_dataset.zip"
            
            # Mock successful download and unzip
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_response.iter_content.return_value = [b"fake zip content"]
            mock_requests.return_value = mock_response
            mock_unzip.return_value = True
            
            # Call download_dataset with force_public_cache=True
            success, result_path = download_dataset(
                dataset_id=dataset_id,
                force_public_cache=True,
                config=config
            )
            
            # Should succeed and return public path
            assert success, "Download should succeed"
            public_path = _get_dataset_path(dataset_id, config=config, is_public=True)
            assert str(public_path) == result_path, f"Should return public path, got {result_path}"
            
            # Verify public dataset info was fetched
            mock_all_dependencies["fetch_public_info"].assert_called_once_with(
                dataset_id, None, None, config
            )
            
            # Verify public download was attempted
            mock_requests.assert_called_once()
            mock_unzip.assert_called_once()
    
    def test_private_download_skips_public_check_when_local_exists(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that private download doesn't check public datasets when local version exists.
        """
        dataset_id = "test/dataset"
        config = test_config_with_public_private
        
        # Create existing private dataset
        private_path = _get_dataset_path(dataset_id, config=config, is_public=False)
        create_mock_dataset(private_path, is_public=False)
        
        # Call download_dataset without force_public_cache or make_public
        success, result_path = download_dataset(
            dataset_id=dataset_id,
            config=config
        )
        
        # Should succeed and return private path
        assert success, "Download should succeed"
        assert str(private_path) == result_path, f"Should return private path, got {result_path}"
        
        # Should not check for public datasets since local version exists
        mock_all_dependencies["fetch_public_info"].assert_not_called()
        
        captured = capsys.readouterr()
        assert "already exists in private cache" in captured.out
    
    def test_both_force_public_cache_and_make_public_true(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that both force_public_cache=True and make_public=True work together.
        """
        dataset_id = "test/dataset"
        config = test_config_with_public_private
        
        # Create existing private dataset
        private_path = _get_dataset_path(dataset_id, config=config, is_public=False)
        create_mock_dataset(private_path, is_public=False)
        
        # Call download_dataset with both flags
        success, result_path = download_dataset(
            dataset_id=dataset_id,
            force_public_cache=True,
            make_public=True,
            config=config
        )
        
        # Should succeed and return public path
        assert success, "Download should succeed"
        public_path = _get_dataset_path(dataset_id, config=config, is_public=True)
        assert str(public_path) == result_path, f"Should return public path, got {result_path}"
        
        # Verify HF download was called
        mock_all_dependencies["load_dataset"].assert_called_once()
        
        # Verify save_to_disk was called with public path
        mock_all_dependencies["dataset_instance"].save_to_disk.assert_called_once_with(str(public_path))
    
    def test_config_name_and_revision_handled_correctly_in_public_cache(
        self, test_config_with_public_private, mock_all_dependencies, capsys
    ):
        """
        Test that config_name and revision are handled correctly for public cache.
        """
        dataset_id = "test/dataset"
        config_name = "custom_config"
        revision = "v1.0"
        config = test_config_with_public_private
        
        # Create existing private dataset with different config/revision
        private_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=False)
        create_mock_dataset(private_path, is_public=False)
        
        # Call download_dataset with force_public_cache=True and specific config/revision
        success, result_path = download_dataset(
            dataset_id=dataset_id,
            config_name=config_name,
            revision=revision,
            force_public_cache=True,
            config=config
        )
        
        # Should succeed and return public path with correct config/revision
        assert success, "Download should succeed"
        public_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=True)
        assert str(public_path) == result_path, f"Should return public path, got {result_path}"
        
        # Verify HF download was called with correct parameters
        mock_all_dependencies["load_dataset"].assert_called_once_with(
            path=dataset_id, name=config_name, revision=revision, trust_remote_code=False
        ) 