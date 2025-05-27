import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class HGLocalizationConfig:
    """Configuration class for HG Localization that can be populated from environment variables or other sources."""
    
    def __init__(
        self,
        s3_bucket_name: Optional[str] = None,
        s3_endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        s3_data_prefix: str = "",
        datasets_store_path: Optional[Path] = None,
        default_config_name: str = "default_config",
        default_revision_name: str = "default_revision",
        public_datasets_json_key: str = "public_datasets.json",
        public_datasets_zip_dir_prefix: str = "public_datasets_zip"
    ):
        """
        Initialize configuration.
        
        Args:
            s3_bucket_name: S3 bucket name for dataset storage
            s3_endpoint_url: S3 endpoint URL (for custom S3-compatible services)
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            s3_data_prefix: Root prefix in the S3 bucket for all data
            datasets_store_path: Local path for dataset storage
            default_config_name: Default configuration name for datasets
            default_revision_name: Default revision name for datasets
            public_datasets_json_key: S3 key for public datasets manifest
            public_datasets_zip_dir_prefix: S3 prefix for public dataset zips
        """
        self.s3_bucket_name = s3_bucket_name
        self.s3_endpoint_url = s3_endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_data_prefix = s3_data_prefix.strip('/') if s3_data_prefix else ""
        
        # Set default datasets store path if not provided
        if datasets_store_path is None:
            _default_store_path_parent = Path(__file__).parent
            self.datasets_store_path = Path(os.environ.get(
                "HGLOC_DATASETS_STORE_PATH", 
                _default_store_path_parent / "datasets_store"
            ))
        else:
            self.datasets_store_path = datasets_store_path
            
        self.default_config_name = default_config_name
        self.default_revision_name = default_revision_name
        self.public_datasets_json_key = public_datasets_json_key
        self.public_datasets_zip_dir_prefix = public_datasets_zip_dir_prefix
    
    @property
    def public_datasets_store_path(self) -> Path:
        """Get the path for storing public datasets (subdirectory under main store)"""
        return self.datasets_store_path / "public"
    
    @classmethod
    def from_env(cls) -> 'HGLocalizationConfig':
        """Create configuration from environment variables."""
        return cls(
            s3_bucket_name=os.environ.get("HGLOC_S3_BUCKET_NAME"),
            s3_endpoint_url=os.environ.get("HGLOC_S3_ENDPOINT_URL"),
            aws_access_key_id=os.environ.get("HGLOC_AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("HGLOC_AWS_SECRET_ACCESS_KEY"),
            s3_data_prefix=os.environ.get("HGLOC_S3_DATA_PREFIX", ""),
            datasets_store_path=None,  # Will use default logic in __init__
            default_config_name=os.environ.get("HGLOC_DEFAULT_CONFIG_NAME", "default_config"),
            default_revision_name=os.environ.get("HGLOC_DEFAULT_REVISION_NAME", "default_revision"),
            public_datasets_json_key=os.environ.get("HGLOC_PUBLIC_DATASETS_JSON_KEY", "public_datasets.json"),
            public_datasets_zip_dir_prefix=os.environ.get("HGLOC_PUBLIC_DATASETS_ZIP_DIR_PREFIX", "public_datasets_zip")
        )
    
    def is_s3_configured(self) -> bool:
        """Check if S3 is properly configured."""
        return bool(
            self.s3_bucket_name and 
            self.aws_access_key_id and 
            self.aws_secret_access_key
        )
    
    def has_credentials(self) -> bool:
        """Check if AWS credentials are available for private access."""
        return bool(self.aws_access_key_id and self.aws_secret_access_key)


# Create a default configuration instance from environment variables
# This maintains backward compatibility for existing code
default_config = HGLocalizationConfig.from_env()

# Backward compatibility: expose the old global variables
# These can be gradually phased out as code is updated to use the config object
S3_BUCKET_NAME = default_config.s3_bucket_name
S3_ENDPOINT_URL = default_config.s3_endpoint_url
AWS_ACCESS_KEY_ID = default_config.aws_access_key_id
AWS_SECRET_ACCESS_KEY = default_config.aws_secret_access_key
S3_DATA_PREFIX = default_config.s3_data_prefix
DATASETS_STORE_PATH = default_config.datasets_store_path
DEFAULT_CONFIG_NAME = default_config.default_config_name
DEFAULT_REVISION_NAME = default_config.default_revision_name
PUBLIC_DATASETS_JSON_KEY = default_config.public_datasets_json_key
PUBLIC_DATASETS_ZIP_DIR_PREFIX = default_config.public_datasets_zip_dir_prefix 