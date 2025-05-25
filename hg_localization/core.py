"""Core functionalities for Hugging Face dataset localization with S3 support."""

# Import configurations and constants first to make them available to other modules if needed
from .config import (
    S3_BUCKET_NAME, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, S3_DATA_PREFIX,
    DATASETS_STORE_PATH,
    DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME,
    PUBLIC_DATASETS_JSON_KEY, PUBLIC_DATASETS_ZIP_DIR_PREFIX
)

# Import utility functions (these are mostly internal)
# from .utils import _get_safe_path_component, _zip_directory, _unzip_file 
# No, utils are used by dataset_manager and s3_utils, not directly exposed typically.

# Import S3 utility functions (mostly internal, but some might be useful externally)
from .s3_utils import (
    get_s3_dataset_card_presigned_url # Example of a potentially useful external S3 util
)

# Import primary public API functions from dataset_manager
from .dataset_manager import (
    download_dataset,
    load_local_dataset,
    upload_dataset,
    list_local_datasets,
    list_s3_datasets,
    sync_local_dataset_to_s3,
    sync_all_local_to_s3,
    get_dataset_card_url,
    get_dataset_card_content,
    get_cached_dataset_card_content
)

# Define __all__ for explicit public API exposure
__all__ = [
    # Configs (optional to expose, depends on library usage philosophy)
    "S3_BUCKET_NAME",
    "S3_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "S3_DATA_PREFIX",
    "DATASETS_STORE_PATH",
    "DEFAULT_CONFIG_NAME",
    "DEFAULT_REVISION_NAME",
    "PUBLIC_DATASETS_JSON_KEY",
    "PUBLIC_DATASETS_ZIP_DIR_PREFIX",
    
    # Dataset Management Functions
    "download_dataset",
    "load_local_dataset",
    "upload_dataset",
    "list_local_datasets",
    "list_s3_datasets",
    "sync_local_dataset_to_s3",
    "sync_all_local_to_s3",

    # Dataset Card Utilities
    "get_dataset_card_url",
    "get_dataset_card_content",
    "get_cached_dataset_card_content",
    "get_s3_dataset_card_presigned_url"
]

# Cleanup (optional - remove the old core.py if this is replacing it)
# This script would typically be part of a larger refactoring script.
# For this interactive step, we assume core.py is deleted or renamed manually if __init__.py takes its place. 