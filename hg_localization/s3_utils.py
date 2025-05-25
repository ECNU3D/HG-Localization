import os
import json
from pathlib import Path
from typing import Optional, Any, Dict

import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, ClientError

# Import S3 specific configurations and general utils
from .config import (
    S3_BUCKET_NAME, S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, S3_DATA_PREFIX, 
    PUBLIC_DATASETS_JSON_KEY, PUBLIC_DATASETS_ZIP_DIR_PREFIX
)
from .utils import _get_safe_path_component # If _get_safe_path_component is in utils.py

# --- S3 Client and Core S3 Operations ---

def _get_s3_client() -> Optional[Any]: # boto3.client type hint can be tricky
    """Initializes and returns an S3 client if configuration is valid and credentials are provided."""
    if not S3_BUCKET_NAME:
        return None
    
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        return None

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=S3_ENDPOINT_URL, 
            config=Config(s3={"addressing_style": "virtual", "aws_chunked_encoding_enabled": False},
                          signature_version='v4')
        )
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME) 
        return s3_client
    except NoCredentialsError:
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

def _get_s3_prefix(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None) -> str:
    """Constructs the S3 prefix for a dataset version, including the global S3_DATA_PREFIX."""
    from .config import DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME # Delayed import to avoid circular if utils imports config
    
    safe_dataset_id = _get_safe_path_component(dataset_id)
    safe_config_name = _get_safe_path_component(config_name if config_name else DEFAULT_CONFIG_NAME)
    safe_revision = _get_safe_path_component(revision if revision else DEFAULT_REVISION_NAME)
    
    base_dataset_s3_prefix = f"{safe_dataset_id}/{safe_config_name}/{safe_revision}"
    
    if S3_DATA_PREFIX:
        return f"{S3_DATA_PREFIX.strip('/')}/{base_dataset_s3_prefix}"
    return base_dataset_s3_prefix

def _get_prefixed_s3_key(base_key: str) -> str:
    """Constructs the full S3 key by prepending S3_DATA_PREFIX if set."""
    stripped_base_key = base_key.lstrip('/')
    if S3_DATA_PREFIX:
        return f"{S3_DATA_PREFIX.strip('/')}/{stripped_base_key}"
    return stripped_base_key

def _check_s3_dataset_exists(s3_client: Any, bucket_name: str, s3_prefix_for_dataset_version: str) -> bool:
    """Checks if a dataset (marker files) exists at the given S3 prefix for the dataset version."""
    if not s3_client or not bucket_name:
        return False
    try:
        s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix_for_dataset_version.rstrip('/')}/dataset_info.json")
        return True
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == '404':
            try:
                s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix_for_dataset_version.rstrip('/')}/dataset_dict.json")
                return True
            except ClientError as e2:
                if e2.response.get('Error', {}).get('Code') == '404':
                    return False
                return False
        return False
    except Exception:
        return False

def _upload_directory_to_s3(s3_client: Any, local_directory: Path, s3_bucket: str, s3_prefix_for_upload: str):
    """Uploads a directory to S3, maintaining structure under the given s3_prefix_for_upload."""
    print(f"Uploading {local_directory} to s3://{s3_bucket}/{s3_prefix_for_upload}...")
    for item in local_directory.rglob('*'):
        if item.is_file():
            s3_key = f"{s3_prefix_for_upload.rstrip('/')}/{item.relative_to(local_directory).as_posix()}"
            try:
                print(f"  Uploading {item.name} to {s3_key}")
                s3_client.upload_file(str(item), s3_bucket, s3_key)
                print(f"  Uploaded {item.name} to {s3_key}")
            except Exception as e:
                print(f"  Failed to upload {item.name}: {e}")
    print("Upload complete.")

def _download_directory_from_s3(s3_client: Any, local_directory: Path, s3_bucket: str, s3_prefix_to_download: str) -> bool:
    """Downloads a directory from S3 specified by s3_prefix_to_download."""
    print(f"Attempting to download s3://{s3_bucket}/{s3_prefix_to_download} to {local_directory}...")
    os.makedirs(local_directory, exist_ok=True)
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix_to_download.rstrip('/') + '/')
    files_downloaded = 0
    try:
        for page in pages:
            if "Contents" not in page:
                print(f"  No objects found in s3://{s3_bucket}/{s3_prefix_to_download}")
                return False
            for obj in page['Contents']:
                s3_key = obj['Key']
                if s3_key.endswith('/'): 
                    continue
                
                # Path relative to the s3_prefix_to_download
                relative_key_path = Path(s3_key).relative_to(Path(s3_prefix_to_download))
                local_file_path = local_directory / relative_key_path
                os.makedirs(local_file_path.parent, exist_ok=True)
                print(f"  Downloading {s3_key} to {local_file_path}...")
                s3_client.download_file(s3_bucket, s3_key, str(local_file_path))
                files_downloaded += 1
        if files_downloaded == 0:
             print(f"  No files were actually downloaded from s3://{s3_bucket}/{s3_prefix_to_download}.")
             return False
        print(f"Successfully downloaded {files_downloaded} files from S3.")
        return True
    except ClientError as e:
        print(f"S3 Error during download from prefix '{s3_prefix_to_download}': {e}")
        return False
    except Exception as e:
        print(f"Error downloading from S3 prefix '{s3_prefix_to_download}': {e}")
        return False

# --- Public Datasets Manifest (public_datasets.json) Utilities ---

def _update_public_datasets_json(s3_client: Any, bucket_name: str, dataset_id: str, config_name: Optional[str], revision: Optional[str], zip_s3_key_relative_to_prefix: str) -> bool:
    """Updates the public_datasets.json file in S3."""
    from .config import DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME # Delayed import
    if not s3_client: return False
    
    current_config = {}
    full_json_s3_key = _get_prefixed_s3_key(PUBLIC_DATASETS_JSON_KEY)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_json_s3_key)
        current_config = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"{full_json_s3_key} not found in S3, will create a new one.")
        else:
            print(f"Error fetching {full_json_s3_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {full_json_s3_key} in S3 is corrupted. Will overwrite.")
        current_config = {}

    entry_key = f"{dataset_id}---{config_name or DEFAULT_CONFIG_NAME}---{revision or DEFAULT_REVISION_NAME}"
    current_config[entry_key] = {
        "dataset_id": dataset_id,
        "config_name": config_name,
        "revision": revision,
        "s3_zip_key": zip_s3_key_relative_to_prefix, # This is the key relative to S3_DATA_PREFIX (or root)
        "s3_bucket": bucket_name
    }

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=full_json_s3_key,
            Body=json.dumps(current_config, indent=2),
            ContentType='application/json',
            ACL='public-read'
        )
        print(f"Successfully updated and published {full_json_s3_key} in S3.")
        return True
    except Exception as e:
        print(f"Error uploading {full_json_s3_key} to S3: {e}")
        return False

def _get_s3_public_url(bucket_name: str, s3_key: str, endpoint_url: Optional[str] = None) -> str:
    """Constructs a public HTTPS URL for an S3 object. s3_key is the full key (including any S3_DATA_PREFIX path)."""
    if endpoint_url:
        scheme = "https://" 
        if endpoint_url.startswith("http://"):
            scheme = "http://"
        host_part = endpoint_url.replace("https://", "").replace("http://", "")
        clean_host_part = host_part.rstrip('/')
        return f"{scheme}{bucket_name}.{clean_host_part}/{s3_key.lstrip('/')}"
    else:
        return f"https://{bucket_name}.s3.amazonaws.com/{s3_key.lstrip('/')}"

def get_s3_dataset_card_presigned_url(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, expires_in: int = 3600) -> Optional[str]:
    """Generates a pre-signed URL for the dataset_card.md in the private S3 path."""
    s3_client = _get_s3_client()
    version_str = f"(dataset: {dataset_id}, config: {config_name or 'default'}, revision: {revision or 'default'})"

    if not s3_client or not S3_BUCKET_NAME:
        print(f"S3 client not available or bucket not configured. Cannot generate presigned URL for dataset card {version_str}.")
        return None

    # The s3_prefix_path already includes S3_DATA_PREFIX if set
    s3_prefix_path_for_dataset_version = _get_s3_prefix(dataset_id, config_name, revision)
    s3_card_key_full = f"{s3_prefix_path_for_dataset_version.rstrip('/')}/dataset_card.md"

    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_card_key_full)
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_card_key_full},
            ExpiresIn=expires_in
        )
        print(f"Generated presigned URL for dataset card {s3_card_key_full}: {url}")
        return url
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == '404':
            print(f"Cannot generate presigned URL: Dataset card not found on S3 at {s3_card_key_full}")
        else:
            print(f"S3 ClientError when checking/generating presigned URL for {s3_card_key_full}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error generating presigned URL for {s3_card_key_full}: {e}")
        return None 