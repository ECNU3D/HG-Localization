import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dotenv import load_dotenv
import zipfile
import json
import requests
from huggingface_hub import ModelCard

load_dotenv()

import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, ClientError
from botocore import UNSIGNED
from datasets import load_dataset, load_from_disk, DatasetDict, Dataset

# Configuration from Environment Variables
S3_BUCKET_NAME = os.environ.get("HGLOC_S3_BUCKET_NAME")
S3_ENDPOINT_URL = os.environ.get("HGLOC_S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("HGLOC_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("HGLOC_AWS_SECRET_ACCESS_KEY")
# AWS_DEFAULT_REGION = os.environ.get("HGLOC_AWS_DEFAULT_REGION")
S3_DATA_PREFIX = os.environ.get("HGLOC_S3_DATA_PREFIX", "").strip('/') # User-configurable root prefix in the bucket

DATASETS_STORE_PATH = Path(os.environ.get("HGLOC_DATASETS_STORE_PATH", Path(__file__).parent / "datasets_store"))

DEFAULT_CONFIG_NAME = "default_config"
DEFAULT_REVISION_NAME = "default_revision"

PUBLIC_DATASETS_JSON_KEY = "public_datasets.json"
PUBLIC_DATASETS_ZIP_DIR_PREFIX = "public_datasets_zip" # S3 prefix for storing zips

# --- Path and Naming Utilities ---

def _get_safe_path_component(name: Optional[str]) -> str:
    if not name: return ""
    return name.replace("/", "_").replace("\\\\", "_")

def _get_dataset_path(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None) -> Path:
    safe_dataset_id = _get_safe_path_component(dataset_id)
    safe_config_name = _get_safe_path_component(config_name if config_name else DEFAULT_CONFIG_NAME)
    safe_revision = _get_safe_path_component(revision if revision else DEFAULT_REVISION_NAME)
    return DATASETS_STORE_PATH / safe_dataset_id / safe_config_name / safe_revision

def _get_s3_prefix(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None) -> str:
    safe_dataset_id = _get_safe_path_component(dataset_id)
    safe_config_name = _get_safe_path_component(config_name if config_name else DEFAULT_CONFIG_NAME)
    safe_revision = _get_safe_path_component(revision if revision else DEFAULT_REVISION_NAME)
    base_s3_prefix = f"{safe_dataset_id}/{safe_config_name}/{safe_revision}"
    if S3_DATA_PREFIX:
        return f"{S3_DATA_PREFIX}/{base_s3_prefix}"
    return base_s3_prefix

def _get_prefixed_s3_key(base_key: str) -> str:
    """Constructs the full S3 key by prepending S3_DATA_PREFIX if set."""
    stripped_base_key = base_key.lstrip('/')
    if S3_DATA_PREFIX:
        return f"{S3_DATA_PREFIX}/{stripped_base_key}"
    return stripped_base_key

# --- S3 Client and Core S3 Operations ---

def _get_s3_client() -> Optional[Any]: # boto3.client type hint can be tricky
    """Initializes and returns an S3 client if configuration is valid and credentials are provided."""
    if not S3_BUCKET_NAME:
        # print("S3_BUCKET_NAME not configured. S3 operations will be skipped.")
        return None
    
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        # print("S3 credentials (HGLOC_AWS_ACCESS_KEY_ID, HGLOC_AWS_SECRET_ACCESS_KEY) not found. Cannot create authenticated S3 client.")
        return None # No credentials, no client for authenticated operations

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=S3_ENDPOINT_URL, # Will be None if not set, which is fine for AWS S3
            # region_name=AWS_DEFAULT_REGION
            config=Config(s3={"addressing_style": "virtual", "aws_chunked_encoding_enabled": False},
                          signature_version='v4')
        )
        # Perform a quick check to see if credentials and config are likely valid
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME) # Throws ClientError on issues
        return s3_client
    except NoCredentialsError: # Should be caught by the check above, but as a safeguard
        print("S3 Error: AWS credentials not found during client initialization.")
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"S3 Error: Bucket '{S3_BUCKET_NAME}' does not exist.")
        elif e.response['Error']['Code'] == 'InvalidAccessKeyId' or e.response['Error']['Code'] == 'SignatureDoesNotMatch':
             print(f"S3 Error: Invalid AWS credentials provided.")
        else:
            print(f"S3 ClientError during client test: {e}")
        return None
    except Exception as e:
        print(f"Error initializing S3 client: {e}")
        return None

def _check_s3_dataset_exists(s3_client: Any, bucket_name: str, s3_prefix: str) -> bool:
    """Checks if a dataset (marker files) exists at the given S3 prefix."""
    if not s3_client or not bucket_name:
        return False
    try:
        # Check for dataset_info.json
        s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix.rstrip('/')}/dataset_info.json")
        return True
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == '404':
            # dataset_info.json not found, try dataset_dict.json
            try:
                s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix.rstrip('/')}/dataset_dict.json")
                return True
            except ClientError as e2:
                if e2.response.get('Error', {}).get('Code') == '404':
                    return False # Neither found
                else:
                    # print(f"S3 Warning: Could not check {s3_prefix}/dataset_dict.json: {e2}") # Optional: log other errors
                    return False #  Treat other errors as "not found" for simplicity here
        else:
            # print(f"S3 Warning: Could not check {s3_prefix}/dataset_info.json: {e}") # Optional: log other errors
            return False # Treat other errors as "not found"
    except Exception: # Catch any other boto3 or general exceptions
        # print(f"S3 Warning: Exception while checking S3 object existence for {s3_prefix}: {e_gen}")
        return False

def _upload_directory_to_s3(s3_client: Any, local_directory: Path, s3_bucket: str, s3_prefix: str):
    """Uploads a directory to S3, maintaining structure."""
    print(f"Uploading {local_directory} to s3://{s3_bucket}/{s3_prefix}...")
    for item in local_directory.rglob('*'):
        if item.is_file():
            s3_key = f"{s3_prefix.rstrip('/')}/{item.relative_to(local_directory).as_posix()}"
            try:
                print(f"  Uploading {item.name} to {s3_key}")
                s3_client.upload_file(str(item), s3_bucket, s3_key)
                print(f"  Uploaded {item.name} to {s3_key}")
            except Exception as e:
                print(f"  Failed to upload {item.name}: {e}")
                # Decide if you want to raise the error or just continue
    print("Upload complete.")

def _download_directory_from_s3(s3_client: Any, local_directory: Path, s3_bucket: str, s3_prefix: str) -> bool:
    """Downloads a directory from S3."""
    print(f"Attempting to download s3://{s3_bucket}/{s3_prefix} to {local_directory}...")
    os.makedirs(local_directory, exist_ok=True)
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix.rstrip('/') + '/')
    files_downloaded = 0
    try:
        for page in pages:
            if "Contents" not in page:
                print(f"  No objects found in s3://{s3_bucket}/{s3_prefix}")
                return False # No files found at prefix
            for obj in page['Contents']:
                s3_key = obj['Key']
                # Ensure we don't try to create a file from a "directory" object if S3 represents them
                if s3_key.endswith('/'): 
                    continue
                # Construct relative path carefully to avoid issues if s3_prefix is just dataset_name
                # and key is dataset_name/config_name/revision_name/file.txt
                # We want the path relative to the s3_prefix which represents the dataset_version root
                path_parts = Path(s3_key).parts
                prefix_parts_len = len(Path(s3_prefix).parts)
                relative_path_in_s3_version_folder = Path(*path_parts[prefix_parts_len:])

                local_file_path = local_directory / relative_path_in_s3_version_folder
                os.makedirs(local_file_path.parent, exist_ok=True)
                print(f"  Downloading {s3_key} to {local_file_path}...")
                s3_client.download_file(s3_bucket, s3_key, str(local_file_path))
                files_downloaded += 1
        if files_downloaded == 0:
             print(f"  No files were actually downloaded from s3://{s3_bucket}/{s3_prefix}. The prefix might exist but be empty or contain only folders.")
             return False
        print(f"Successfully downloaded {files_downloaded} files from S3.")
        return True
    except ClientError as e:
        print(f"S3 Error during download from prefix '{s3_prefix}': {e}")
        return False
    except Exception as e:
        print(f"Error downloading from S3 prefix '{s3_prefix}': {e}")
        return False

# --- ZIP Utilities ---

def _zip_directory(directory_path: Path, zip_path: Path) -> bool:
    """Zips the contents of a directory."""
    if not directory_path.is_dir():
        print(f"Error: {directory_path} is not a valid directory to zip.")
        return False
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for item in directory_path.rglob('*'):
                arcname = item.relative_to(directory_path)
                zipf.write(item, arcname=arcname)
        print(f"Successfully zipped {directory_path} to {zip_path}")
        return True
    except Exception as e:
        print(f"Error zipping directory {directory_path}: {e}")
        return False

def _unzip_file(zip_path: Path, extract_to_path: Path) -> bool:
    """Unzips a file to a specified directory."""
    if not zip_path.is_file():
        print(f"Error: {zip_path} is not a valid zip file.")
        return False
    os.makedirs(extract_to_path, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            zipf.extractall(extract_to_path)
        print(f"Successfully unzipped {zip_path} to {extract_to_path}")
        return True
    except Exception as e:
        print(f"Error unzipping file {zip_path}: {e}")
        return False

# --- Public Datasets Manifest (public_datasets.json) Utilities ---

def _update_public_datasets_json(s3_client: Any, bucket_name: str, dataset_id: str, config_name: Optional[str], revision: Optional[str], zip_s3_key: str) -> bool:
    """Updates the public_datasets.json file in S3."""
    if not s3_client: return False
    
    current_config = {}
    prefixed_json_key = _get_prefixed_s3_key(PUBLIC_DATASETS_JSON_KEY)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=prefixed_json_key)
        current_config = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"{prefixed_json_key} not found in S3, will create a new one.")
        else:
            print(f"Error fetching {prefixed_json_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {prefixed_json_key} in S3 is corrupted. Will overwrite.")
        current_config = {} # Start fresh if corrupted

    entry_key = f"{dataset_id}---{config_name or DEFAULT_CONFIG_NAME}---{revision or DEFAULT_REVISION_NAME}"
    current_config[entry_key] = {
        "dataset_id": dataset_id,
        "config_name": config_name,
        "revision": revision,
        "s3_zip_key": zip_s3_key,
        "s3_bucket": bucket_name # Store bucket name for easier anonymous access later
    }

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=prefixed_json_key,
            Body=json.dumps(current_config, indent=2),
            ContentType='application/json',
            ACL='public-read' # Make the JSON itself public
        )
        print(f"Successfully updated and published {prefixed_json_key} in S3.")
        return True
    except Exception as e:
        print(f"Error uploading {prefixed_json_key} to S3: {e}")
        return False

def _get_s3_public_url(bucket_name: str, key: str, endpoint_url: Optional[str] = None) -> str:
    """Constructs a public HTTPS URL for an S3 object."""
    if endpoint_url: # For S3-compatible storage like MinIO
        scheme = ""
        host_part = endpoint_url

        if host_part.startswith("https://"):
            scheme = "https://"
            host_part = host_part[len("https://"):]
        elif host_part.startswith("http://"):
            scheme = "https://" # Always upgrade to https for public URLs
            host_part = host_part[len("http://"):]
        else:
            # If no scheme is provided in endpoint_url, default to https for public URLs
            scheme = "https://"
        
        # Remove any trailing slash from the host part (e.g., minio.example.com:9000/ -> minio.example.com:9000)
        clean_host_part = host_part.rstrip('/')

        # Construct virtual-hosted style URL: e.g., https://mybucket.minio.example.com:9000/mykey
        return f"{scheme}{bucket_name}.{clean_host_part}/{key.lstrip('/')}"
    else: # AWS S3 default
        # Using virtual-hosted style for AWS S3 as it's common and generally preferred.
        # This assumes the bucket name is DNS compliant.
        # An alternative is path-style: f"https://s3.amazonaws.com/{bucket_name}/{key.lstrip('/')}" 
        # (add region if known and consistently required: f"https://s3.{AWS_DEFAULT_REGION}.amazonaws.com/...") # AWS_DEFAULT_REGION is commented out
        return f"https://{bucket_name}.s3.amazonaws.com/{key.lstrip('/')}"

def _fetch_public_datasets_json_via_url() -> Optional[Dict[str, Any]]:
    """Fetches the public_datasets.json via direct HTTPS GET if S3_BUCKET_NAME is set."""
    if not S3_BUCKET_NAME:
        print("S3_BUCKET_NAME not configured. Cannot fetch public datasets JSON.")
        return None

    prefixed_json_key = _get_prefixed_s3_key(PUBLIC_DATASETS_JSON_KEY)
    json_url = _get_s3_public_url(S3_BUCKET_NAME, prefixed_json_key, S3_ENDPOINT_URL)
    print(f"Attempting to fetch public datasets JSON from: {json_url}")
    try:
        response = requests.get(json_url, timeout=10) # 10 second timeout
        response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error fetching {json_url}: {e}")
        if e.response.status_code == 404:
            print(f"{prefixed_json_key} not found at the public URL.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {json_url}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Content at {json_url} is not valid JSON.")
        return None

def _fetch_public_dataset_info(dataset_id: str, config_name: Optional[str], revision: Optional[str]) -> Optional[Dict[str, str]]:
    """Fetches a specific dataset's public zip info from public_datasets.json using direct URL access."""
    public_config = _fetch_public_datasets_json_via_url()
    if not public_config:
        return None
        
    entry_key = f"{dataset_id}---{config_name or DEFAULT_CONFIG_NAME}---{revision or DEFAULT_REVISION_NAME}"
    dataset_info = public_config.get(entry_key)
    if dataset_info:
        # Validate that essential keys are present
        if not all(k in dataset_info for k in ["s3_zip_key", "s3_bucket"]):
            print(f"Error: Public dataset info for {entry_key} is incomplete. Missing 's3_zip_key' or 's3_bucket'.")
            return None
        print(f"Found public dataset info for {entry_key}: {dataset_info}")
        return dataset_info
    else:
        print(f"Public dataset info not found for {entry_key} in {PUBLIC_DATASETS_JSON_KEY}")
        return None

# --- Core Public API Functions ---

def download_dataset(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, trust_remote_code: bool = False, make_public: bool = False, skip_s3_upload: bool = False) -> Tuple[bool, str]:
    version_str = f"(config: {config_name or 'default'}, revision: {revision or 'default'})"
    print(f"Processing dataset: {dataset_id} {version_str}")
    local_save_path = _get_dataset_path(dataset_id, config_name, revision)

    # 1. Check local cache first (enhanced)
    if local_save_path.exists() and \
       ((local_save_path / "dataset_info.json").exists() or \
        (local_save_path / "dataset_dict.json").exists()):
        print(f"Dataset {dataset_id} {version_str} already exists locally at {local_save_path}")
        return True, str(local_save_path)

    # 2. If not local, check S3 (if configured)
    s3_client = _get_s3_client()
    if s3_client and S3_BUCKET_NAME:
        s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision)
        print(f"Checking S3 for dataset {dataset_id} {version_str} at s3://{S3_BUCKET_NAME}/{s3_prefix_path}...")
        if _check_s3_dataset_exists(s3_client, S3_BUCKET_NAME, s3_prefix_path):
            print(f"Dataset found on S3. Attempting to download from S3 to local cache: {local_save_path}...")
            if _download_directory_from_s3(s3_client, local_save_path, S3_BUCKET_NAME, s3_prefix_path):
                print(f"Successfully downloaded dataset from S3 to {local_save_path}.")
                # Verify it's actually there after download (paranoid check, _download_directory_from_s3 should ensure it)
                if local_save_path.exists() and \
                   ((local_save_path / "dataset_info.json").exists() or \
                    (local_save_path / "dataset_dict.json").exists()):
                    return True, str(local_save_path)
                else:
                    print(f"Error: S3 download reported success, but dataset not found or incomplete at {local_save_path}. Proceeding to Hugging Face.")
            else:
                print(f"Failed to download dataset from S3. Will attempt Hugging Face download.")
        else:
            print(f"Dataset not found on S3. Will attempt Hugging Face download.")
    else:
        print("S3 not configured or client init failed; skipping S3 check.")

    # 3. Download from Hugging Face
    config_str_part = f" config '{config_name}'" if config_name else ""
    revision_str_part = f" (revision: {revision})" if revision else ""
    print(f"Downloading dataset '{dataset_id}'{config_str_part} from Hugging Face{revision_str_part}...")
    try:
        downloaded_hf_dataset = load_dataset(path=dataset_id, name=config_name, revision=revision, trust_remote_code=trust_remote_code)
        print("Hugging Face download complete.")

        print(f"Saving dataset to local cache at {local_save_path}...")
        os.makedirs(local_save_path, exist_ok=True)
        downloaded_hf_dataset.save_to_disk(str(local_save_path))
        print(f"Dataset '{dataset_id}' {version_str} successfully saved to local cache: {local_save_path}")

        # Attempt to fetch and save the dataset card
        card_content_md = get_dataset_card_content(dataset_id, revision=revision)
        if card_content_md:
            card_file_path = local_save_path / "dataset_card.md"
            try:
                with open(card_file_path, "w", encoding="utf-8") as f:
                    f.write(card_content_md)
                print(f"Successfully saved dataset card to {card_file_path}")
            except IOError as e:
                print(f"Warning: Failed to save dataset card to {card_file_path}: {e}")
        else:
            print(f"Warning: Could not retrieve dataset card for {dataset_id} {version_str}. It will not be saved locally or to S3.")

        if skip_s3_upload:
            print(f"Skipping S3 upload for {dataset_id} {version_str} as requested.")
        else:
            s3_client_for_upload = _get_s3_client() # Re-get client in case initial one was for read-only/public checks
            if s3_client_for_upload and S3_BUCKET_NAME:
                s3_prefix_path_for_upload = _get_s3_prefix(dataset_id, config_name, revision) # Use consistent prefix
                _upload_directory_to_s3(s3_client_for_upload, local_save_path, S3_BUCKET_NAME, s3_prefix_path_for_upload)

                if make_public:
                    print(f"Preparing to make dataset {dataset_id} {version_str} public...")
                    safe_dataset_id = _get_safe_path_component(dataset_id)
                    safe_config_name = _get_safe_path_component(config_name if config_name else DEFAULT_CONFIG_NAME)
                    safe_revision = _get_safe_path_component(revision if revision else DEFAULT_REVISION_NAME)
                    
                    zip_file_name = f"{safe_dataset_id}---{safe_config_name}---{safe_revision}.zip"
                    base_s3_zip_key = f"{PUBLIC_DATASETS_ZIP_DIR_PREFIX}/{zip_file_name}" # Key relative to S3_DATA_PREFIX
                    s3_zip_key_full = _get_prefixed_s3_key(base_s3_zip_key) # Full key for upload
                    
                    # Create a temporary directory for zipping to avoid including parent folders
                    with tempfile.TemporaryDirectory() as tmp_zip_src_dir:
                        tmp_dataset_path_for_zip = Path(tmp_zip_src_dir) / local_save_path.name # This line from original was commented out
                        shutil.copytree(local_save_path, tmp_dataset_path_for_zip) # This line from original was commented out
                        
                        # Zip the contents of tmp_dataset_path_for_zip
                        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
                            tmp_zip_file_path = Path(tmp_zip_file.name)
                            if _zip_directory(tmp_dataset_path_for_zip, tmp_zip_file_path): # Zip contents of tmp_dataset_path_for_zip
                                print(f"Uploading public zip {tmp_zip_file_path} to s3://{S3_BUCKET_NAME}/{s3_zip_key_full}")
                                try:
                                    s3_client_for_upload.upload_file(
                                        str(tmp_zip_file_path), 
                                        S3_BUCKET_NAME, 
                                        s3_zip_key_full,
                                        ExtraArgs={'ACL': 'public-read'}
                                    )
                                    print(f"Successfully uploaded public zip to {s3_zip_key_full}")
                                    # Pass the base_s3_zip_key (relative to S3_DATA_PREFIX or root if no prefix) to JSON
                                    _update_public_datasets_json(s3_client_for_upload, S3_BUCKET_NAME, dataset_id, config_name, revision, base_s3_zip_key)
                                except Exception as e:
                                    print(f"Failed to upload public zip {s3_zip_key_full}: {e}")
                            else:
                                print(f"Failed to zip dataset for public upload.")
                            # Clean up temp zip file
                            try:
                                os.remove(tmp_zip_file_path)
                            except OSError:
                                pass
        return True, str(local_save_path)

    except FileNotFoundError:
        # Corrected f-string for FileNotFoundError message
        error_msg_detail = f"Dataset '{dataset_id}'{config_str_part} not found on Hugging Face Hub."
        print(f"Error: {error_msg_detail}")
        return False, error_msg_detail
    except Exception as e:
        print(f"An error occurred while processing '{dataset_id}' {version_str}: {e}")
        if local_save_path.exists(): # Check before trying to delete
            try:
                shutil.rmtree(local_save_path)
                print(f"Cleaned up partially saved data at {local_save_path}")
            except Exception as cleanup_e:
                print(f"Error during cleanup of {local_save_path}: {cleanup_e}")
        return False, str(e)

def load_local_dataset(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None) -> Optional[DatasetDict | Dataset]:
    print(f"Loading dataset: {dataset_id} {config_name} {revision}")
    local_dataset_path = _get_dataset_path(dataset_id, config_name, revision)
    print(f"local_dataset_path: {local_dataset_path}")
    version_str = f"(config: {config_name or DEFAULT_CONFIG_NAME.replace('_',' ')}, revision: {revision or DEFAULT_REVISION_NAME.replace('_',' ')})"

    if not (local_dataset_path.exists() and 
            ((local_dataset_path / "dataset_info.json").exists() or 
             (local_dataset_path / "dataset_dict.json").exists())):
        print(f"Dataset '{dataset_id}' {version_str} not found in local cache at {local_dataset_path}.")
        
        s3_download_attempted = False
        # Attempt S3 fetch with credentials first if available
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY: # Check if creds are configured
            s3_client_auth = _get_s3_client() # Attempt to get an authenticated client
            if s3_client_auth and S3_BUCKET_NAME:
                print("Attempting to fetch from S3 using credentials...")
                s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision)
                if _download_directory_from_s3(s3_client_auth, local_dataset_path, S3_BUCKET_NAME, s3_prefix_path):
                    print(f"Successfully downloaded from S3 (authenticated) to {local_dataset_path}.")
                    s3_download_attempted = True 
                else:
                    print(f"Failed to download '{dataset_id}' {version_str} from S3 (authenticated) or not found.")
            else: 
                print("S3 client could not be initialized for authenticated download (check credentials and bucket name).")
        
        if not (local_dataset_path.exists() and \
                ((local_dataset_path / "dataset_info.json").exists() or \
                 (local_dataset_path / "dataset_dict.json").exists())):
            print("Attempting to fetch from public S3 dataset list via URL...")
            public_info = _fetch_public_dataset_info(dataset_id, config_name, revision)
            if public_info and public_info.get('s3_zip_key') and public_info.get('s3_bucket'):
                public_s3_bucket = public_info['s3_bucket']
                public_s3_zip_key_relative = public_info['s3_zip_key'] 

                full_public_s3_zip_key = public_s3_zip_key_relative 
                
                target_bucket_for_url = S3_BUCKET_NAME if S3_BUCKET_NAME else public_s3_bucket

                if S3_BUCKET_NAME and public_s3_bucket != S3_BUCKET_NAME:
                     print(f"Warning: Public dataset info bucket '{public_s3_bucket}' differs from configured HGLOC_S3_BUCKET_NAME '{S3_BUCKET_NAME}'. Using '{target_bucket_for_url}' for download URL.")
                
                zip_url = _get_s3_public_url(target_bucket_for_url, full_public_s3_zip_key, S3_ENDPOINT_URL)
                print(f"Public dataset zip found. Attempting download from: {zip_url}")
                
                with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_download_zip_file:
                    tmp_zip_file_path = Path(tmp_download_zip_file.name)
                    try:
                        response = requests.get(zip_url, stream=True, timeout=300) 
                        response.raise_for_status()
                        for chunk in response.iter_content(chunk_size=8192):
                            tmp_download_zip_file.write(chunk)
                        
                        tmp_download_zip_file.close() 

                        print(f"Public zip downloaded to {tmp_zip_file_path}. Unzipping...")
                        os.makedirs(local_dataset_path, exist_ok=True)
                        if _unzip_file(tmp_zip_file_path, local_dataset_path):
                            print(f"Successfully downloaded and unzipped public dataset to {local_dataset_path}.")
                        else:
                            print(f"Failed to unzip public dataset from {tmp_zip_file_path}.")
                            if local_dataset_path.exists(): shutil.rmtree(local_dataset_path)
                    except requests.exceptions.HTTPError as e:
                        print(f"HTTP error downloading public zip {zip_url}: {e}")
                        if local_dataset_path.exists(): shutil.rmtree(local_dataset_path)
                    except requests.exceptions.RequestException as e:
                        print(f"Error downloading public zip {zip_url}: {e}")
                        if local_dataset_path.exists(): shutil.rmtree(local_dataset_path)
                    except Exception as e:
                        print(f"Unexpected error during public S3 zip download/unzip: {e}")
                        if local_dataset_path.exists(): shutil.rmtree(local_dataset_path)
                    finally:
                        if not tmp_download_zip_file.closed: tmp_download_zip_file.close()
                        try:
                            os.remove(tmp_zip_file_path)
                        except OSError: pass
            else: 
                if not (local_dataset_path.exists() and \
                        ((local_dataset_path / "dataset_info.json").exists() or \
                         (local_dataset_path / "dataset_dict.json").exists())):
                    print(f"Dataset '{dataset_id}' {version_str} not found in public S3 dataset list or info was incomplete.")

        if not (local_dataset_path.exists() and 
                ((local_dataset_path / "dataset_info.json").exists() or 
                 (local_dataset_path / "dataset_dict.json").exists())):
            print(f"Dataset '{dataset_id}' {version_str} could not be fetched from any source.")
            return None
    
    try:
        print(f"Loading dataset '{dataset_id}' {version_str} from {local_dataset_path}...")
        loaded_data = load_from_disk(str(local_dataset_path))
        print("Dataset loaded successfully from local path.")
        return loaded_data
    except Exception as e:
        print(f"An error occurred while loading '{dataset_id}' {version_str} from {local_dataset_path}: {e}")
        return None

def upload_dataset(dataset_obj: Dataset | DatasetDict, dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, make_public: bool = False) -> bool:
    """
    Saves a given Hugging Face Dataset or DatasetDict object to the local cache
    and then uploads it to S3 if S3 is configured.

    Args:
        dataset_obj: The Hugging Face Dataset or DatasetDict object to save and upload.
        dataset_id: The unique identifier for the dataset.
        config_name: Optional configuration name for the dataset.
        revision: Optional revision or version for the dataset.
        make_public: If True, zips the dataset and uploads it to a public S3 location,
                     then updates public_datasets.json. Requires S3 to be configured.

    Returns:
        True if the dataset was successfully saved locally and S3 operations (if applicable) succeeded or were skipped.
        False if local save failed or a required S3 operation failed.
    """
    version_str = f"(config: {config_name or 'default'}, revision: {revision or 'default'})"
    print(f"Processing dataset for upload: {dataset_id} {version_str}")
    local_save_path = _get_dataset_path(dataset_id, config_name, revision)

    try:
        print(f"Saving dataset to local cache at {local_save_path}...")
        os.makedirs(local_save_path, exist_ok=True)
        dataset_obj.save_to_disk(str(local_save_path))
        print(f"Dataset '{dataset_id}' {version_str} successfully saved to local cache: {local_save_path}")
    except Exception as e:
        print(f"Error saving dataset '{dataset_id}' {version_str} to {local_save_path}: {e}")
        return False

    s3_client = _get_s3_client()
    if s3_client and S3_BUCKET_NAME:
        s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision)
        print(f"Attempting to upload dataset from {local_save_path} to S3: s3://{S3_BUCKET_NAME}/{s3_prefix_path}")
        try:
            _upload_directory_to_s3(s3_client, local_save_path, S3_BUCKET_NAME, s3_prefix_path)
            print(f"Successfully initiated upload of dataset '{dataset_id}' {version_str} to S3.")
            
            if make_public:
                print(f"Preparing to make (uploaded) dataset {dataset_id} {version_str} public...")
                safe_dataset_id = _get_safe_path_component(dataset_id)
                safe_config_name = _get_safe_path_component(config_name if config_name else DEFAULT_CONFIG_NAME)
                safe_revision = _get_safe_path_component(revision if revision else DEFAULT_REVISION_NAME)
                
                zip_file_name = f"{safe_dataset_id}---{safe_config_name}---{safe_revision}.zip"
                base_s3_zip_key = f"{PUBLIC_DATASETS_ZIP_DIR_PREFIX}/{zip_file_name}" # Relative to S3_DATA_PREFIX
                s3_zip_key_full = _get_prefixed_s3_key(base_s3_zip_key) # Full key for upload
                
                with tempfile.TemporaryDirectory() as tmp_zip_src_dir: # This temp dir is from original code
                    tmp_dataset_path_for_zip = Path(tmp_zip_src_dir) / local_save_path.name # This line from original
                    shutil.copytree(local_save_path, tmp_dataset_path_for_zip) # This line from original

                    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
                        tmp_zip_file_path = Path(tmp_zip_file.name)
                        if _zip_directory(tmp_dataset_path_for_zip, tmp_zip_file_path): # Zipping the copied content
                            print(f"Uploading public zip {tmp_zip_file_path} to s3://{S3_BUCKET_NAME}/{s3_zip_key_full}")
                            try:
                                s3_client.upload_file(
                                    str(tmp_zip_file_path), 
                                    S3_BUCKET_NAME, 
                                    s3_zip_key_full,
                                    ExtraArgs={'ACL': 'public-read'}
                                )
                                print(f"Successfully uploaded public zip to {s3_zip_key_full}")
                                _update_public_datasets_json(s3_client, S3_BUCKET_NAME, dataset_id, config_name, revision, base_s3_zip_key)
                            except Exception as e:
                                print(f"Failed to upload public zip {s3_zip_key_full}: {e}")
                        else:
                            print(f"Failed to zip dataset for public upload.")
                        try:
                            os.remove(tmp_zip_file_path)
                        except OSError:
                            pass
            return True 
        except Exception as e:
            print(f"Error uploading dataset '{dataset_id}' {version_str} to S3: {e}")
            return False 
    else: 
        print("S3 not configured or client init failed; skipping S3 upload. Dataset is saved locally.")
        if make_public:
            print("Cannot make dataset public as S3 is not configured.")
        return True

# --- Sync Local to S3 Functions ---

def sync_local_dataset_to_s3(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, make_public: bool = False) -> Tuple[bool, str]:
    """
    Syncs a single locally stored dataset to S3 if it's not already present or if making public is requested.

    Checks for the dataset in the local cache, then checks S3. If not on S3 (private copy),
    it uploads. If `make_public` is True, it ensures the public ZIP and manifest entry exist,
    creating them if necessary.

    Args:
        dataset_id: The dataset identifier.
        config_name: Optional configuration name.
        revision: Optional revision.
        make_public: If True, also create/update the public S3 zip and manifest entry.

    Returns:
        A tuple (bool, str) indicating success and a message.
    """
    version_str = f"(config: {config_name or 'default'}, revision: {revision or 'default'})"
    print(f"Attempting to sync local dataset to S3: {dataset_id} {version_str}")

    local_save_path = _get_dataset_path(dataset_id, config_name, revision)

    if not (local_save_path.exists() and \
            ((local_save_path / "dataset_info.json").exists() or \
             (local_save_path / "dataset_dict.json").exists())):
        msg = f"Local dataset {dataset_id} {version_str} not found or is incomplete at {local_save_path}. Cannot sync."
        print(msg)
        return False, msg

    s3_client = _get_s3_client()
    if not s3_client or not S3_BUCKET_NAME:
        msg = "S3 not configured (bucket name or client init failed). Cannot sync to S3."
        if make_public:
            msg += " Cannot make dataset public."
        print(msg)
        return False, msg

    s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision)
    private_s3_copy_exists = _check_s3_dataset_exists(s3_client, S3_BUCKET_NAME, s3_prefix_path)

    if private_s3_copy_exists:
        print(f"Dataset {dataset_id} {version_str} already exists as private S3 copy at s3://{S3_BUCKET_NAME}/{s3_prefix_path}.")
    else:
        print(f"Dataset {dataset_id} {version_str} not found on S3 (private). Uploading from {local_save_path} to s3://{S3_BUCKET_NAME}/{s3_prefix_path}")
        try:
            _upload_directory_to_s3(s3_client, local_save_path, S3_BUCKET_NAME, s3_prefix_path)
            print(f"Successfully uploaded dataset '{dataset_id}' {version_str} to S3 (private).")
            private_s3_copy_exists = True # Now it exists
        except Exception as e:
            msg = f"Error uploading dataset '{dataset_id}' {version_str} to S3 (private): {e}"
            print(msg)
            return False, msg # If private upload fails, and it didn't exist, then we can't proceed.

    # Handle --make-public if the private copy exists (either pre-existing or just uploaded)
    if make_public and private_s3_copy_exists:
        print(f"Processing --make-public for dataset {dataset_id} {version_str}...")
        
        safe_dataset_id = _get_safe_path_component(dataset_id)
        safe_config_name = _get_safe_path_component(config_name if config_name else DEFAULT_CONFIG_NAME)
        safe_revision = _get_safe_path_component(revision if revision else DEFAULT_REVISION_NAME)
        
        zip_file_name = f"{safe_dataset_id}---{safe_config_name}---{safe_revision}.zip"
        base_s3_zip_key = f"{PUBLIC_DATASETS_ZIP_DIR_PREFIX}/{zip_file_name}" 
        s3_zip_key_full = _get_prefixed_s3_key(base_s3_zip_key)
        
        public_zip_uploaded_or_existed = False
        try:
            s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_zip_key_full)
            print(f"Public zip s3://{S3_BUCKET_NAME}/{s3_zip_key_full} already exists.")
            public_zip_uploaded_or_existed = True
        except ClientError as e:
            if e.response['Error']['Code'] == '404': # Not found
                print(f"Public zip s3://{S3_BUCKET_NAME}/{s3_zip_key_full} not found. Will attempt to create and upload.")
                # Proceed to create and upload
                with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
                    tmp_zip_file_path = Path(tmp_zip_file.name)
                    zip_creation_upload_success = False
                    try:
                        if _zip_directory(local_save_path, tmp_zip_file_path):
                            print(f"Uploading public zip {tmp_zip_file_path.name} to s3://{S3_BUCKET_NAME}/{s3_zip_key_full}")
                            s3_client.upload_file(
                                str(tmp_zip_file_path), 
                                S3_BUCKET_NAME, 
                                s3_zip_key_full,
                                ExtraArgs={'ACL': 'public-read'}
                            )
                            print(f"Successfully uploaded public zip to {s3_zip_key_full}")
                            public_zip_uploaded_or_existed = True
                            zip_creation_upload_success = True
                        else:
                            print(f"Failed to zip dataset at {local_save_path} for public upload.")
                    except Exception as ex_zip_upload:
                        print(f"Failed during public zip creation/upload for {s3_zip_key_full}: {ex_zip_upload}")
                    finally:
                        try:
                            os.remove(tmp_zip_file_path) 
                        except OSError: pass
                    
                    if not zip_creation_upload_success:
                        print(f"Skipping manifest update for {dataset_id} {version_str} due to zip creation/upload failure.")
            else: # Other error during head_object
                print(f"Error checking for existing public zip {s3_zip_key_full}: {e}. Skipping make_public actions for this dataset.")

        if public_zip_uploaded_or_existed:
            print(f"Updating public_datasets.json for {dataset_id} {version_str} with zip key {base_s3_zip_key}")
            if not _update_public_datasets_json(s3_client, S3_BUCKET_NAME, dataset_id, config_name, revision, base_s3_zip_key):
                print(f"Warning: Failed to update public datasets JSON for {dataset_id} {version_str}, though public zip should exist at {s3_zip_key_full}.")
    elif make_public and not private_s3_copy_exists:
        # This case should ideally not be hit if logic above is correct (private upload must succeed first)
        print(f"Cannot make {dataset_id} {version_str} public because its private S3 copy does not exist or failed to upload.")

    final_msg = f"Sync process for {dataset_id} {version_str} completed."
    if private_s3_copy_exists:
        final_msg += " Private S3 copy is present."
    else:
        # This should mean the initial check found it, but then something went wrong, or it was never uploaded.
        # The function should have returned False earlier if upload failed.
        final_msg += " Private S3 copy was NOT successfully synced/found."


    if make_public:
        final_msg += " Public versioning was processed (see logs for details)."
    
    print(final_msg)
    return True, final_msg # Returns True if local dataset existed and S3 client was available, errors in S3 ops are logged.

def sync_all_local_to_s3(make_public: bool = False) -> None:
    """
    Iterates through all local datasets and attempts to sync them to S3.
    For each dataset, it uploads if not present on S3 (private copy).
    If `make_public` is True, it also attempts to create/update the public S3 zip and manifest.

    Args:
        make_public: If True, also attempts to make each synced dataset public.
    """
    print(f"Starting sync of all local datasets to S3. Make public: {make_public}")
    local_datasets = list_local_datasets()
    if not local_datasets:
        print("No local datasets found in cache to sync.")
        return

    succeeded_syncs = 0
    failed_syncs = 0
    
    s3_client_check = _get_s3_client()
    if not s3_client_check or not S3_BUCKET_NAME:
        print("S3 not configured (bucket name or client init failed). Cannot sync any datasets to S3.")
        if make_public:
            print("Cannot make datasets public.")
        return

    for ds_info in local_datasets:
        dataset_id = ds_info['dataset_id']
        config_name = ds_info.get('config_name') # Handles None if default
        revision = ds_info.get('revision')     # Handles None if default
        
        print(f"\n--- Processing local dataset for sync: ID='{dataset_id}', Config='{config_name}', Revision='{revision}' ---")
        success, message = sync_local_dataset_to_s3(dataset_id, config_name, revision, make_public=make_public)
        if success:
            succeeded_syncs += 1
        else:
            failed_syncs += 1
            # Message already printed by sync_local_dataset_to_s3
            
    print(f"\n--- Sync all local datasets to S3 finished ---")
    print(f"Total local datasets processed: {len(local_datasets)}")
    print(f"Successfully processed (primary sync action): {succeeded_syncs}")
    print(f"Failed to process (see logs for errors): {failed_syncs}")

# --- List Local Datasets ---
def list_local_datasets() -> List[Dict[str, str]]:
    available_datasets = []
    if not DATASETS_STORE_PATH.exists():
        print(f"Local dataset store directory does not exist: {DATASETS_STORE_PATH}")
        return available_datasets
    
    for dataset_id_dir in DATASETS_STORE_PATH.iterdir():
        if dataset_id_dir.is_dir():
            if dataset_id_dir.name == ".gitkeep" or dataset_id_dir.name.startswith("."):
                continue
            for config_name_dir in dataset_id_dir.iterdir():
                if config_name_dir.is_dir():
                    for revision_dir in config_name_dir.iterdir():
                        has_info_json = (revision_dir / "dataset_info.json").exists()
                        has_dict_json = (revision_dir / "dataset_dict.json").exists()
                        if revision_dir.is_dir() and (has_info_json or has_dict_json):
                            dataset_id_display = dataset_id_dir.name
                            config_name_display = config_name_dir.name
                            revision_display = revision_dir.name
                            available_datasets.append({
                                "dataset_id": dataset_id_display,
                                "config_name": config_name_display if config_name_display != DEFAULT_CONFIG_NAME else None,
                                "revision": revision_display if revision_display != DEFAULT_REVISION_NAME else None
                            })
    if not available_datasets:
        print("No local datasets found in cache.")
    return available_datasets

def list_s3_datasets() -> List[Dict[str, str]]:
    """Lists datasets available in S3. 
    If S3 credentials are set, it scans the bucket. 
    Otherwise, if only S3_BUCKET_NAME is set, it attempts to list from public_datasets.json.
    """
    available_s3_datasets = []

    if not S3_BUCKET_NAME:
        print("S3_BUCKET_NAME not configured. Cannot list S3 datasets.")
        return []

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        s3_client = _get_s3_client()
        if s3_client:
            print("Listing S3 datasets via authenticated API call (scanning bucket structure)...")
            paginator = s3_client.get_paginator('list_objects_v2')
            scan_base_prefix = S3_DATA_PREFIX + '/' if S3_DATA_PREFIX and S3_DATA_PREFIX != "/" else (S3_DATA_PREFIX if S3_DATA_PREFIX == "/" else "") # Handle S3_DATA_PREFIX correctly
            if S3_DATA_PREFIX and not S3_DATA_PREFIX.endswith('/') and S3_DATA_PREFIX != "": # Ensure trailing slash if not empty and not just "/"
                 scan_base_prefix = S3_DATA_PREFIX + '/'
            elif not S3_DATA_PREFIX: # if S3_DATA_PREFIX is empty string
                 scan_base_prefix = ""

            try:
                for page1 in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=scan_base_prefix, Delimiter='/'):
                    for common_prefix1 in page1.get('CommonPrefixes', []):
                        dataset_id_full_prefix = common_prefix1.get('Prefix', '')
                        if not dataset_id_full_prefix.endswith('/'): continue

                        relative_to_scan_base = dataset_id_full_prefix[len(scan_base_prefix):].strip('/')
                        if not relative_to_scan_base: continue
                        dataset_id = relative_to_scan_base

                        for page2 in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=dataset_id_full_prefix, Delimiter='/'):
                            for common_prefix2 in page2.get('CommonPrefixes', []):
                                config_name_full_prefix = common_prefix2.get('Prefix', '')
                                if not config_name_full_prefix.endswith('/'): continue
                                
                                relative_to_dataset_id_prefix = config_name_full_prefix[len(dataset_id_full_prefix):].strip('/')
                                if not relative_to_dataset_id_prefix: continue
                                config_name = relative_to_dataset_id_prefix

                                for page3 in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=config_name_full_prefix, Delimiter='/'):
                                    for common_prefix3 in page3.get('CommonPrefixes', []):
                                        revision_full_prefix = common_prefix3.get('Prefix', '')
                                        if not revision_full_prefix.endswith('/'): continue

                                        relative_to_config_name_prefix = revision_full_prefix[len(config_name_full_prefix):].strip('/')
                                        if not relative_to_config_name_prefix: continue
                                        revision = relative_to_config_name_prefix
                                        
                                        current_dataset_version_s3_prefix = revision_full_prefix.rstrip('/')

                                        if _check_s3_dataset_exists(s3_client, S3_BUCKET_NAME, current_dataset_version_s3_prefix):
                                            s3_card_url = get_s3_dataset_card_presigned_url(
                                                dataset_id=dataset_id,
                                                config_name=config_name, # Original scanned config name
                                                revision=revision        # Original scanned revision
                                            )
                                            available_s3_datasets.append({
                                                "dataset_id": dataset_id,
                                                "config_name": config_name if config_name != DEFAULT_CONFIG_NAME else None,
                                                "revision": revision if revision != DEFAULT_REVISION_NAME else None,
                                                "s3_card_url": s3_card_url # Add the presigned URL
                                            })
                if available_s3_datasets:
                    print("Successfully listed S3 datasets by scanning bucket structure.")
                    return available_s3_datasets
                print("No datasets found by scanning S3 bucket structure (or structure did not match expected format).")

            except ClientError as e:
                print(f"Error listing S3 datasets via API: {e}. Falling back to check public list if applicable.")
        else:
            print("S3 client could not be initialized despite credentials appearing to be set. Proceeding to check public list.")

    print("Attempting to list S3 datasets from public_datasets.json...")
    public_json_content = _fetch_public_datasets_json_via_url()
    if public_json_content:
        if not available_s3_datasets: 
            public_datasets_from_json = []
            for entry_key, entry_data in public_json_content.items():
                if isinstance(entry_data, dict) and all(k in entry_data for k in ["dataset_id", "s3_zip_key"]):
                    public_datasets_from_json.append({
                        "dataset_id": entry_data.get("dataset_id"),
                        "config_name": entry_data.get("config_name"), 
                        "revision": entry_data.get("revision"),
                        "s3_card_url": None # Card URL not available directly from public JSON listing
                    })
                else:
                    print(f"Skipping malformed entry in public_datasets.json: {entry_key}")
            
            if public_datasets_from_json:
                print("Listing S3 datasets based on public_datasets.json.")
                available_s3_datasets = public_datasets_from_json
            else:
                print(f"No datasets found or listed in {PUBLIC_DATASETS_JSON_KEY} at public URL (or it was empty).")
    else:
        print(f"Could not fetch or parse {PUBLIC_DATASETS_JSON_KEY} from public URL.")
    
    if not available_s3_datasets:
        print(f"No datasets found in S3 bucket '{S3_BUCKET_NAME}' by scanning or from public list.")
    return available_s3_datasets

# --- Dataset Card Utilities ---

def get_dataset_card_url(dataset_id: str) -> str:
    """
    Constructs the URL to the dataset's card on the Hugging Face Hub.

    Args:
        dataset_id: The identifier of the dataset (e.g., "glue", "squad").

    Returns:
        The URL to the dataset's card on Hugging Face.
    """
    return f"https://huggingface.co/datasets/{dataset_id}"

def get_dataset_card_content(dataset_id: str, revision: Optional[str] = None) -> Optional[str]:
    """
    Fetches the Markdown content of a dataset card from the Hugging Face Hub.

    Args:
        dataset_id: The identifier of the dataset (e.g., "glue", "squad").
        revision: Optional revision (branch, tag, or commit hash) of the dataset card.

    Returns:
        The Markdown content of the dataset card as a string, or None if not found or an error occurs.
    """
    try:
        # For datasets, the repo_id on the Hub is typically just the dataset_id prefixed with 'datasets/' implicitly by ModelCard.load sometimes
        # However, to be explicit and align with how hf_hub_download works for datasets, 
        # it's often better to specify repo_type="dataset".
        # For ModelCard.load, the expectation is usually the direct repo_id. If it's a dataset, it's just dataset_id.
        # The huggingface_hub library handles whether it's a model or dataset based on the repo_id format or explicit repo_type.
        # When loading a card for a dataset, it should be just `dataset_id`
        print(f"Attempting to load dataset card for: {dataset_id} (revision: {revision or 'main'})")
        card = ModelCard.load(dataset_id, repo_type="dataset")
        return card.text # card.text excludes the YAML metadata header, card.content includes it
    except Exception as e:
        # More specific exceptions can be caught, e.g., huggingface_hub.utils.RepositoryNotFoundError
        print(f"Error loading dataset card for '{dataset_id}' (revision: {revision or 'main'}): {e}")
        return None

def get_cached_dataset_card_content(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None) -> Optional[str]:
    """
    Retrieves dataset card content, prioritizing local cache, then private S3.
    This function does NOT attempt to fetch from Hugging Face directly.

    Args:
        dataset_id: The dataset identifier.
        config_name: Optional configuration name.
        revision: Optional revision.

    Returns:
        The Markdown content of the dataset card, or None if not found.
    """
    local_dataset_dir = _get_dataset_path(dataset_id, config_name, revision)
    local_card_file_path = local_dataset_dir / "dataset_card.md"
    version_str = f"(dataset: {dataset_id}, config: {config_name or 'default'}, revision: {revision or 'default'})"

    if local_card_file_path.exists() and local_card_file_path.is_file():
        print(f"Found dataset card locally for {version_str} at {local_card_file_path}")
        try:
            with open(local_card_file_path, "r", encoding="utf-8") as f:
                return f.read()
        except IOError as e:
            print(f"Error reading local dataset card {local_card_file_path}: {e}")
            # Proceed to check S3 if local read fails

    print(f"Dataset card not found or readable locally for {version_str}. Checking S3 (private path)...")
    s3_client = _get_s3_client()
    if s3_client and S3_BUCKET_NAME:
        s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision)
        s3_card_key = f"{s3_prefix_path.rstrip('/')}/dataset_card.md"
        
        try:
            print(f"Attempting to download dataset card from S3: s3://{S3_BUCKET_NAME}/{s3_card_key}")
            # Ensure local directory exists before downloading
            os.makedirs(local_dataset_dir, exist_ok=True)
            s3_client.download_file(S3_BUCKET_NAME, s3_card_key, str(local_card_file_path))
            print(f"Successfully downloaded dataset card from S3 to {local_card_file_path}")
            # Now read the newly downloaded file
            with open(local_card_file_path, "r", encoding="utf-8") as f:
                return f.read()
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                print(f"Dataset card not found on S3 at {s3_card_key}")
            else:
                print(f"S3 ClientError when trying to download dataset card {s3_card_key}: {e}")
            return None
        except IOError as e:
            print(f"IOError after downloading dataset card from S3 {local_card_file_path}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error downloading/reading dataset card from S3 {s3_card_key}: {e}")
            return None
    else:
        print("S3 client not available or bucket not configured. Cannot fetch dataset card from S3.")
        return None

def get_s3_dataset_card_presigned_url(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, expires_in: int = 3600) -> Optional[str]:
    """
    Generates a pre-signed URL for the dataset_card.md in the private S3 path.

    Args:
        dataset_id: The dataset identifier.
        config_name: Optional configuration name.
        revision: Optional revision.
        expires_in: Time in seconds for the presigned URL to remain valid. Default 1 hour.

    Returns:
        A pre-signed URL string, or None if S3 client is not available or an error occurs.
    """
    s3_client = _get_s3_client()
    version_str = f"(dataset: {dataset_id}, config: {config_name or 'default'}, revision: {revision or 'default'})"

    if not s3_client or not S3_BUCKET_NAME:
        print(f"S3 client not available or bucket not configured. Cannot generate presigned URL for dataset card {version_str}.")
        return None

    s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision)
    s3_card_key = f"{s3_prefix_path.rstrip('/')}/dataset_card.md"

    try:
        # First, check if the object actually exists. 
        # generate_presigned_url doesn't fail if the key doesn't exist.
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_card_key)
        
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_card_key},
            ExpiresIn=expires_in
        )
        print(f"Generated presigned URL for dataset card {s3_card_key}: {url}")
        return url
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == '404':
            print(f"Cannot generate presigned URL: Dataset card not found on S3 at {s3_card_key}")
        else:
            print(f"S3 ClientError when checking/generating presigned URL for {s3_card_key}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error generating presigned URL for {s3_card_key}: {e}")
        return None

# --- Example Usage ---
# Example usage (optional, for testing directly)
if __name__ == '__main__':
    print("--- HG-Localization S3 Integration Test Script ---")
    print(f"Local cache path: {DATASETS_STORE_PATH}")
    if not S3_BUCKET_NAME:
        print("\nWARNING: HGLOC_S3_BUCKET_NAME is not set. S3 tests will be skipped or limited.")
        print("Please set HGLOC_S3_BUCKET_NAME, HGLOC_AWS_ACCESS_KEY_ID, HGLOC_AWS_SECRET_ACCESS_KEY, and optionally HGLOC_S3_ENDPOINT_URL & HGLOC_AWS_DEFAULT_REGION.")
    else:
        print(f"S3 Bucket: {S3_BUCKET_NAME}")
        print(f"S3 Endpoint: {S3_ENDPOINT_URL if S3_ENDPOINT_URL else 'Default AWS'}")
        print(f"S3 Data Prefix: '{S3_DATA_PREFIX}' (if empty, means root of bucket)")

    # Ensure local datasets_store exists for caching
    if not DATASETS_STORE_PATH.exists():
        os.makedirs(DATASETS_STORE_PATH)
        print(f"Created local cache directory: {DATASETS_STORE_PATH}")

    # Test dataset (use a very small one)
    # For example, "eliezerp/poli" is small. Or a specific config of GLUE like "boolq"
    test_dataset_id = "glue"
    test_config_name = "mrpc" # Example: using a config name
    test_revision = None       # Example: using default revision

    print("\n--- Testing Download (and potential S3 Upload) ---")
    # This will download from HF, save locally, then upload to S3
    dl_success, dl_msg = download_dataset(test_dataset_id, config_name=test_config_name, revision=test_revision, trust_remote_code=True)
    if dl_success:
        print(f"Download to local cache successful: {dl_msg}")
    else:
        print(f"Download to local cache failed: {dl_msg}")

    print("\n--- Testing List Local Datasets ---")
    local_sets = list_local_datasets()
    if local_sets:
        print("Available local datasets:")
        for ds_info in local_sets:
            print(f"  - ID: {ds_info['dataset_id']}, Config: {ds_info['config_name']}, Revision: {ds_info['revision']}")
    
    if S3_BUCKET_NAME: # Only run S3 list if configured
        print("\n--- Testing List S3 Datasets ---")
        s3_sets = list_s3_datasets()
        if s3_sets:
            print("Available S3 datasets:")
            for ds_info in s3_sets:
                print(f"  - ID: {ds_info['dataset_id']}, Config: {ds_info['config_name']}, Revision: {ds_info['revision']}")

    print("\n--- Testing Load (will try local cache first, then S3) ---")
    # To truly test S3 download, you might want to clear the local cache for this dataset first
    # For example: 
    # local_test_ds_path = _get_dataset_path(test_ds_name, test_ds_revision)
    # if local_test_ds_path.exists():
    #     print(f"Temporarily removing {local_test_ds_path} to test S3 download...")
    #     shutil.rmtree(local_test_ds_path)

    loaded_data = load_local_dataset(test_dataset_id, config_name=test_config_name, revision=test_revision)
    if loaded_data:
        print(f"Successfully loaded {test_dataset_id} (config: {test_config_name or 'default'}). Type: {type(loaded_data)}")
        if isinstance(loaded_data, DatasetDict):
            for split_name, data_split in loaded_data.items():
                print(f"  Split '{split_name}': {len(data_split)} examples")
        elif isinstance(loaded_data, Dataset):
            print(f"  Dataset: {len(loaded_data)} examples")
    else:
        print(f"Failed to load {test_dataset_id}.")

    print("\n--- Testing Get Dataset Card URL ---")
    card_url = get_dataset_card_url(test_dataset_id)
    print(f"URL for '{test_dataset_id}' dataset card: {card_url}")

    print("\n--- Testing Get Dataset Card Content ---")
    card_content = get_dataset_card_content(test_dataset_id, revision=test_revision) # Using test_revision
    if card_content:
        print(f"Successfully fetched dataset card content for '{test_dataset_id}'. First 500 chars:")
        print(card_content[:500] + "...")
    else:
        print(f"Could not fetch dataset card content for '{test_dataset_id}'.")

    print("\n--- Testing Get Cached Dataset Card Content ---")
    # Assuming test_dataset_id, test_config_name, test_revision were downloaded and card saved
    cached_card_content = get_cached_dataset_card_content(test_dataset_id, config_name=test_config_name, revision=test_revision)
    if cached_card_content:
        print(f"Successfully fetched cached dataset card content for '{test_dataset_id}'. First 200 chars:")
        print(cached_card_content[:200] + "...")
    else:
        print(f"Could not fetch cached dataset card content for '{test_dataset_id}'.")

    # Test with a non-existent one for cache
    print("\n--- Testing Get Cached Dataset Card Content for Non-Existent ---")
    non_existent_cached_card = get_cached_dataset_card_content("__non_existent_for_cached_card__")
    if non_existent_cached_card is None:
        print("Correctly handled fetching cached card for a non-existent dataset.")
    else:
        print("Error: Fetching cached card for a non-existent dataset did not return None.")

    print("\n--- Testing Get S3 Dataset Card Presigned URL ---")
    # This test assumes the card was uploaded to S3 during download_dataset
    presigned_card_url = get_s3_dataset_card_presigned_url(test_dataset_id, config_name=test_config_name, revision=test_revision)
    if presigned_card_url:
        print(f"Successfully generated presigned URL for '{test_dataset_id}' card: {presigned_card_url}")
    else:
        print(f"Could not generate presigned URL for '{test_dataset_id}' card (it might not be on S3 or S3 is not configured).")

    print("\n--- Testing Load Non-Existent (should fail gracefully) ---")
    non_existent_data = load_local_dataset("__non_existent_dataset__", config_name="__no_config__", revision="__no_revision__")
    if non_existent_data is None:
        print("Correctly handled loading of a non-existent dataset.")
    else:
        print("Error: Loading a non-existent dataset did not return None.") 