import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuration from Environment Variables
S3_BUCKET_NAME = os.environ.get("HGLOC_S3_BUCKET_NAME")
S3_ENDPOINT_URL = os.environ.get("HGLOC_S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("HGLOC_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("HGLOC_AWS_SECRET_ACCESS_KEY")
S3_DATA_PREFIX = os.environ.get("HGLOC_S3_DATA_PREFIX", "").strip('/') # User-configurable root prefix in the bucket

# Path for local dataset storage
# Ensure DATASETS_STORE_PATH is defined before other modules try to use it from here.
# Defaulting to a 'datasets_store' subdirectory next to this config file's presumed location within the package.
# This might need adjustment based on actual package structure and deployment.
# A common pattern is to place it relative to the project root or user's home/cache directory.
# For simplicity here, relative to where 'core.py' (and now its submodules) might be.
_default_store_path_parent = Path(__file__).parent # Assuming config.py is in hg_localization/
DATASETS_STORE_PATH = Path(os.environ.get("HGLOC_DATASETS_STORE_PATH", _default_store_path_parent / "datasets_store"))


# Default names for dataset components if not specified
DEFAULT_CONFIG_NAME = "default_config"
DEFAULT_REVISION_NAME = "default_revision"

# S3 keys and prefixes for public dataset management
PUBLIC_DATASETS_JSON_KEY = "public_datasets.json" # Base key, will be prefixed by S3_DATA_PREFIX
PUBLIC_DATASETS_ZIP_DIR_PREFIX = "public_datasets_zip" # Base S3 prefix for storing zips, will be prefixed by S3_DATA_PREFIX 