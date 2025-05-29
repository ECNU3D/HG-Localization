import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from hg_localization.config import HGLocalizationConfig
from hg_localization.dataset_manager import get_cached_dataset_card_content, _get_dataset_path


class TestDatasetCardPublicAccess:
    """Test suite for dataset card access in public mode"""
    
    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            datasets_store = temp_path / "datasets"
            datasets_store.mkdir(parents=True)
            yield {
                "datasets_store": datasets_store,
                "temp_path": temp_path
            }
    
    @pytest.fixture
    def config_with_bucket(self, temp_dirs):
        """Config with S3 bucket configured"""
        return HGLocalizationConfig(
            s3_bucket_name="test-bucket",
            s3_endpoint_url="https://s3.amazonaws.com",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            datasets_store_path=temp_dirs["datasets_store"]
        )
    
    @pytest.fixture
    def config_no_credentials(self, temp_dirs):
        """Config without credentials (public access only)"""
        return HGLocalizationConfig(
            s3_bucket_name="test-bucket",
            s3_endpoint_url="https://s3.amazonaws.com",
            aws_access_key_id=None,
            aws_secret_access_key=None,
            datasets_store_path=temp_dirs["datasets_store"]
        )
    
    def create_dataset_with_card(self, config, dataset_id, config_name, revision, is_public, card_content):
        """Helper to create a dataset with a card in the appropriate location"""
        dataset_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=is_public)
        dataset_path.mkdir(parents=True, exist_ok=True)
        
        # Create dataset files
        (dataset_path / "dataset_info.json").write_text('{"dataset_name": "test"}')
        (dataset_path / "dataset_card.md").write_text(card_content)
        
        return dataset_path
    
    def test_get_cached_dataset_card_content_public_access_only(self, config_no_credentials):
        """Test that get_cached_dataset_card_content finds card in public cache when public_access_only=True"""
        dataset_id = "test/dataset"
        config_name = "default"
        revision = "main"
        card_content = "# Test Dataset\nThis is a test dataset card."
        
        # Create dataset with card in public cache only
        self.create_dataset_with_card(config_no_credentials, dataset_id, config_name, revision, 
                                    is_public=True, card_content=card_content)
        
        # Test public access mode
        result = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=True
        )
        
        assert result == card_content
    
    def test_get_cached_dataset_card_content_public_access_only_not_found(self, config_no_credentials):
        """Test that get_cached_dataset_card_content returns None when card not in public cache and public_access_only=True"""
        dataset_id = "test/dataset"
        config_name = "default"
        revision = "main"
        card_content = "# Test Dataset\nThis is a test dataset card."
        
        # Create dataset with card in private cache only
        self.create_dataset_with_card(config_no_credentials, dataset_id, config_name, revision, 
                                    is_public=False, card_content=card_content)
        
        # Test public access mode - should not find the card
        result = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=True
        )
        
        assert result is None
    
    def test_get_cached_dataset_card_content_private_access_prefers_public(self, config_with_bucket):
        """Test that get_cached_dataset_card_content prefers public cache when public_access_only=False"""
        dataset_id = "test/dataset"
        config_name = "default"
        revision = "main"
        public_card_content = "# Public Dataset Card"
        private_card_content = "# Private Dataset Card"
        
        # Create dataset with card in both public and private cache
        self.create_dataset_with_card(config_with_bucket, dataset_id, config_name, revision, 
                                    is_public=True, card_content=public_card_content)
        self.create_dataset_with_card(config_with_bucket, dataset_id, config_name, revision, 
                                    is_public=False, card_content=private_card_content)
        
        # Test private access mode - should prefer public cache
        result = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_with_bucket, 
            public_access_only=False
        )
        
        assert result == public_card_content
    
    def test_get_cached_dataset_card_content_private_access_fallback_to_private(self, config_with_bucket):
        """Test that get_cached_dataset_card_content falls back to private cache when public not available"""
        dataset_id = "test/dataset"
        config_name = "default"
        revision = "main"
        private_card_content = "# Private Dataset Card"
        
        # Create dataset with card in private cache only
        self.create_dataset_with_card(config_with_bucket, dataset_id, config_name, revision, 
                                    is_public=False, card_content=private_card_content)
        
        # Test private access mode - should find in private cache
        result = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_with_bucket, 
            public_access_only=False
        )
        
        assert result == private_card_content
    
    def test_regression_public_dataset_card_access_failure_case(self, config_no_credentials):
        """Test the original failure case - public dataset card not found when it should be"""
        dataset_id = "test/public-dataset"
        config_name = "default"
        revision = "main"
        card_content = "# Public Dataset\nThis dataset is publicly available."
        
        # Create a public dataset with card
        self.create_dataset_with_card(config_no_credentials, dataset_id, config_name, revision, 
                                    is_public=True, card_content=card_content)
        
        # Test the new behavior with public_access_only=False - should find the card in public path
        result_private_access = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=False  # This should now find the card in public path
        )
        
        # The new behavior should find the card because it checks public path first
        assert result_private_access == card_content
        
        # Test the new behavior with public_access_only=True - should also succeed
        result_public_access = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=True  # This is the main fix
        )
        
        # The new behavior should find the card in public path
        assert result_public_access == card_content
        
        # Test that the issue was specifically with public-only access
        # Create a dataset only in private cache to show the difference
        private_dataset_id = "test/private-only-dataset"
        self.create_dataset_with_card(config_no_credentials, private_dataset_id, config_name, revision, 
                                    is_public=False, card_content="# Private Dataset")
        
        # Public access mode should NOT find the private-only dataset
        result_public_only_private_dataset = get_cached_dataset_card_content(
            private_dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=True
        )
        assert result_public_only_private_dataset is None
        
        # Private access mode SHOULD find the private-only dataset
        result_private_access_private_dataset = get_cached_dataset_card_content(
            private_dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=False
        )
        assert result_private_access_private_dataset == "# Private Dataset"
    
    def test_dataset_card_with_special_characters_in_path(self, config_no_credentials):
        """Test dataset card access with special characters in dataset ID"""
        dataset_id = "org/dataset-with-special_chars.v2"
        config_name = "special-config"
        revision = "v1.0"
        card_content = "# Special Dataset\nDataset with special characters in path."
        
        # Create dataset with card in public cache
        self.create_dataset_with_card(config_no_credentials, dataset_id, config_name, revision, 
                                    is_public=True, card_content=card_content)
        
        # Test public access mode
        result = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=True
        )
        
        assert result == card_content
    
    def test_dataset_card_with_none_config_and_revision(self, config_no_credentials):
        """Test dataset card access with None config_name and revision"""
        dataset_id = "test/dataset"
        card_content = "# Default Dataset\nDataset with default config and revision."
        
        # Create dataset with card using default values
        self.create_dataset_with_card(config_no_credentials, dataset_id, None, None, 
                                    is_public=True, card_content=card_content)
        
        # Test public access mode with None values
        result = get_cached_dataset_card_content(
            dataset_id, None, None, 
            config=config_no_credentials, 
            public_access_only=True
        )
        
        assert result == card_content
    
    @patch('hg_localization.dataset_manager._get_s3_client')
    def test_dataset_card_s3_download_with_public_access(self, mock_get_s3_client, config_no_credentials):
        """Test that S3 download uses the correct path for public access mode"""
        dataset_id = "test/dataset"
        config_name = "default"
        revision = "main"
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_get_s3_client.return_value = mock_s3_client
        
        # Mock successful S3 download
        def mock_download_file(bucket, key, local_path):
            # Simulate writing the card content to the local path
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(local_path).write_text("# S3 Dataset Card")
        
        mock_s3_client.download_file.side_effect = mock_download_file
        
        # Test public access mode - should download to public path
        result = get_cached_dataset_card_content(
            dataset_id, config_name, revision, 
            config=config_no_credentials, 
            public_access_only=True
        )
        
        assert result == "# S3 Dataset Card"
        
        # Verify the card was downloaded to the public path
        public_path = _get_dataset_path(dataset_id, config_name, revision, config_no_credentials, is_public=True)
        card_path = public_path / "dataset_card.md"
        assert card_path.exists()
        assert card_path.read_text() == "# S3 Dataset Card" 