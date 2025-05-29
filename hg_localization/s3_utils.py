import os
import json
from pathlib import Path
from typing import Optional, Any, Dict

import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, ClientError

# Import the config class and default instance
from .config import HGLocalizationConfig, default_config
from .utils import _get_safe_path_component # If _get_safe_path_component is in utils.py

# --- S3 Client and Core S3 Operations ---

def _get_s3_client(config: Optional[HGLocalizationConfig] = None) -> Optional[Any]: # boto3.client type hint can be tricky
    """Initializes and returns an S3 client if configuration is valid and credentials are provided."""
    if config is None:
        config = default_config
        
    if not config.s3_bucket_name:
        return None
    
    if not config.aws_access_key_id or not config.aws_secret_access_key:
        return None

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            endpoint_url=config.s3_endpoint_url, 
            config=Config(s3={"addressing_style": "virtual", "aws_chunked_encoding_enabled": False},
                          signature_version='v4')
        )
        s3_client.head_bucket(Bucket=config.s3_bucket_name) 
        return s3_client
    except NoCredentialsError:
        print("S3 Error: AWS credentials not found during client initialization.")
        return None
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"S3 Error: Bucket '{config.s3_bucket_name}' does not exist.")
        elif e.response['Error']['Code'] == 'InvalidAccessKeyId' or e.response['Error']['Code'] == 'SignatureDoesNotMatch':
             print(f"S3 Error: Invalid AWS credentials provided.")
        else:
            print(f"S3 ClientError during client test: {e}")
        return None
    except Exception as e:
        print(f"Error initializing S3 client: {e}")
        return None

def _get_s3_prefix(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None) -> str:
    """Constructs the S3 prefix for a dataset version, including the global S3_DATA_PREFIX."""
    if config is None:
        config = default_config
    
    safe_dataset_id = _get_safe_path_component(dataset_id)
    safe_config_name = _get_safe_path_component(config_name if config_name else config.default_config_name)
    safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
    
    base_dataset_s3_prefix = f"{safe_dataset_id}/{safe_config_name}/{safe_revision}"
    
    if config.s3_data_prefix:
        return f"{config.s3_data_prefix}/{base_dataset_s3_prefix}"
    return base_dataset_s3_prefix

def _get_prefixed_s3_key(base_key: str, config: Optional[HGLocalizationConfig] = None) -> str:
    """Constructs the full S3 key by prepending S3_DATA_PREFIX if set."""
    if config is None:
        config = default_config
        
    stripped_base_key = base_key.lstrip('/')
    if config.s3_data_prefix:
        return f"{config.s3_data_prefix}/{stripped_base_key}"
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

def _update_public_datasets_json(s3_client: Any, bucket_name: str, dataset_id: str, config_name: Optional[str], revision: Optional[str], zip_s3_key_relative_to_prefix: str, config: Optional[HGLocalizationConfig] = None) -> bool:
    """Updates the public_datasets.json file in S3."""
    if config is None:
        config = default_config
        
    if not s3_client: return False
    
    current_config_data = {}
    full_json_s3_key = _get_prefixed_s3_key(config.public_datasets_json_key, config)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_json_s3_key)
        current_config_data = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"{full_json_s3_key} not found in S3, will create a new one.")
        else:
            print(f"Error fetching {full_json_s3_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {full_json_s3_key} in S3 is corrupted. Will overwrite.")
        current_config_data = {}

    entry_key = f"{dataset_id}---{config_name or config.default_config_name}---{revision or config.default_revision_name}"
    current_config_data[entry_key] = {
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
            Body=json.dumps(current_config_data, indent=2),
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

def get_s3_dataset_card_presigned_url(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, expires_in: int = 3600, config: Optional[HGLocalizationConfig] = None) -> Optional[str]:
    """Generates a presigned URL for accessing a dataset card stored in S3."""
    if config is None:
        config = default_config
        
    s3_client = _get_s3_client(config)
    if not s3_client or not config.s3_bucket_name:
        print("S3 client not available or bucket not configured. Cannot generate presigned URL.")
        return None

    s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision, config)
    s3_card_key = f"{s3_prefix_path.rstrip('/')}/dataset_card.md"

    try:
        # First check if the dataset card exists
        s3_client.head_object(Bucket=config.s3_bucket_name, Key=s3_card_key)
        
        # If it exists, generate the presigned URL
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': config.s3_bucket_name, 'Key': s3_card_key},
            ExpiresIn=expires_in
        )
        print(f"Generated presigned URL for dataset card {s3_card_key}: {presigned_url}")
        return presigned_url
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Cannot generate presigned URL: Dataset card not found on S3 at {s3_card_key}")
        else:
            print(f"S3 ClientError when checking/generating presigned URL for {s3_card_key}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error generating presigned URL for {s3_card_key}: {e}")
        return None

# --- Public Models Manifest (public_models.json) Utilities ---

def _update_public_models_json(s3_client: Any, bucket_name: str, model_id: str, revision: Optional[str], config: Optional[HGLocalizationConfig] = None) -> bool:
    """Updates the public_models.json file in S3 with model metadata information."""
    if config is None:
        config = default_config
        
    if not s3_client: return False
    
    current_config_data = {}
    full_json_s3_key = _get_prefixed_s3_key(config.public_models_json_key, config)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_json_s3_key)
        current_config_data = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"{full_json_s3_key} not found in S3, will create a new one.")
        else:
            print(f"Error fetching {full_json_s3_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {full_json_s3_key} in S3 is corrupted. Will overwrite.")
        current_config_data = {}

    # For models, we use model_id and revision (no config_name like datasets)
    entry_key = f"{model_id}---{revision or config.default_revision_name}"
    
    # Generate public URLs for model card and config if they exist
    from .model_manager import _get_model_s3_prefix
    s3_prefix_path = _get_model_s3_prefix(model_id, revision, config)
    
    model_card_key = f"{s3_prefix_path}/model_card.md"
    model_config_key = f"{s3_prefix_path}/config.json"
    
    # Check if files exist and generate public URLs
    model_card_url = None
    model_config_url = None
    
    try:
        s3_client.head_object(Bucket=bucket_name, Key=model_card_key)
        model_card_url = _get_s3_public_url(bucket_name, model_card_key, config.s3_endpoint_url)
    except ClientError:
        pass  # Model card doesn't exist
    
    try:
        s3_client.head_object(Bucket=bucket_name, Key=model_config_key)
        model_config_url = _get_s3_public_url(bucket_name, model_config_key, config.s3_endpoint_url)
    except ClientError:
        pass  # Model config doesn't exist
    
    current_config_data[entry_key] = {
        "model_id": model_id,
        "revision": revision,
        "s3_bucket": bucket_name,
        "model_card_url": model_card_url,
        "model_config_url": model_config_url,
        "s3_prefix": s3_prefix_path  # Store the S3 prefix for reference
    }

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=full_json_s3_key,
            Body=json.dumps(current_config_data, indent=2),
            ContentType='application/json',
            ACL='public-read'
        )
        print(f"Successfully updated and published {full_json_s3_key} in S3.")
        return True
    except Exception as e:
        print(f"Error uploading {full_json_s3_key} to S3: {e}")
        return False

def _make_model_metadata_public(s3_client: Any, bucket_name: str, model_id: str, revision: Optional[str], local_model_path: Path, config: Optional[HGLocalizationConfig] = None) -> bool:
    """Makes model metadata files (card and config) public by uploading them with public-read ACL."""
    if config is None:
        config = default_config
        
    if not s3_client or not bucket_name:
        return False
    
    from .model_manager import _get_model_s3_prefix
    s3_prefix_path = _get_model_s3_prefix(model_id, revision, config)
    
    success = True
    
    # Upload model card if it exists
    model_card_file = local_model_path / "model_card.md"
    if model_card_file.exists():
        model_card_s3_key = f"{s3_prefix_path}/model_card.md"
        try:
            print(f"Making model card public: s3://{bucket_name}/{model_card_s3_key}")
            s3_client.upload_file(
                str(model_card_file),
                bucket_name,
                model_card_s3_key,
                ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/markdown'}
            )
            print(f"Successfully made model card public at {model_card_s3_key}")
        except Exception as e:
            print(f"Failed to make model card public: {e}")
            success = False
    
    # Upload model config if it exists
    model_config_file = local_model_path / "config.json"
    if model_config_file.exists():
        model_config_s3_key = f"{s3_prefix_path}/config.json"
        try:
            print(f"Making model config public: s3://{bucket_name}/{model_config_s3_key}")
            s3_client.upload_file(
                str(model_config_file),
                bucket_name,
                model_config_s3_key,
                ExtraArgs={'ACL': 'public-read', 'ContentType': 'application/json'}
            )
            print(f"Successfully made model config public at {model_config_s3_key}")
        except Exception as e:
            print(f"Failed to make model config public: {e}")
            success = False
    
    return success 

# --- Private Datasets Index Utilities ---

def _update_private_datasets_index(s3_client: Any, bucket_name: str, dataset_id: str, config_name: Optional[str], revision: Optional[str], config: Optional[HGLocalizationConfig] = None) -> bool:
    """Updates the private_datasets_index.json file in S3 when a dataset is uploaded privately."""
    if config is None:
        config = default_config
        
    if not s3_client: return False
    
    current_index_data = {}
    full_index_s3_key = _get_prefixed_s3_key(config.private_datasets_index_key, config)
    
    # Try to fetch existing index
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_index_s3_key)
        current_index_data = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"{full_index_s3_key} not found in S3, will create a new one.")
        else:
            print(f"Error fetching {full_index_s3_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {full_index_s3_key} in S3 is corrupted. Will overwrite.")
        current_index_data = {}

    # Create entry key and data
    entry_key = f"{dataset_id}---{config_name or config.default_config_name}---{revision or config.default_revision_name}"
    s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision, config)
    
    # Check if dataset card exists
    s3_card_key = f"{s3_prefix_path}/dataset_card.md"
    has_card = False
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_card_key)
        has_card = True
    except ClientError:
        pass
    
    current_index_data[entry_key] = {
        "dataset_id": dataset_id,
        "config_name": config_name,
        "revision": revision,
        "s3_prefix": s3_prefix_path,
        "s3_bucket": bucket_name,
        "has_card": has_card,
        "last_updated": str(Path(__file__).stat().st_mtime)  # Simple timestamp
    }

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=full_index_s3_key,
            Body=json.dumps(current_index_data, indent=2),
            ContentType='application/json'
            # Note: No ACL='public-read' for private index
        )
        print(f"Successfully updated private datasets index {full_index_s3_key} in S3.")
        return True
    except Exception as e:
        print(f"Error uploading {full_index_s3_key} to S3: {e}")
        return False

def _fetch_private_datasets_index(config: Optional[HGLocalizationConfig] = None) -> Optional[Dict[str, Any]]:
    """Fetches the private_datasets_index.json from S3 using authenticated access."""
    if config is None:
        config = default_config
        
    s3_client = _get_s3_client(config)
    if not s3_client or not config.s3_bucket_name:
        print("S3 client not available or bucket not configured. Cannot fetch private datasets index.")
        return None

    full_index_s3_key = _get_prefixed_s3_key(config.private_datasets_index_key, config)
    
    try:
        print(f"Fetching private datasets index from: s3://{config.s3_bucket_name}/{full_index_s3_key}")
        response = s3_client.get_object(Bucket=config.s3_bucket_name, Key=full_index_s3_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Private datasets index not found at s3://{config.s3_bucket_name}/{full_index_s3_key}")
        else:
            print(f"Error fetching private datasets index: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Private datasets index at s3://{config.s3_bucket_name}/{full_index_s3_key} is not valid JSON.")
        return None
    except Exception as e:
        print(f"Unexpected error fetching private datasets index: {e}")
        return None

def _remove_from_private_datasets_index(s3_client: Any, bucket_name: str, dataset_id: str, config_name: Optional[str], revision: Optional[str], config: Optional[HGLocalizationConfig] = None) -> bool:
    """Removes a dataset entry from the private_datasets_index.json file in S3."""
    if config is None:
        config = default_config
        
    if not s3_client: return False
    
    full_index_s3_key = _get_prefixed_s3_key(config.private_datasets_index_key, config)
    
    # Try to fetch existing index
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_index_s3_key)
        current_index_data = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Private datasets index {full_index_s3_key} not found, nothing to remove.")
            return True  # Consider this success since the entry doesn't exist anyway
        else:
            print(f"Error fetching {full_index_s3_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {full_index_s3_key} in S3 is corrupted.")
        return False

    # Remove entry if it exists
    entry_key = f"{dataset_id}---{config_name or config.default_config_name}---{revision or config.default_revision_name}"
    if entry_key in current_index_data:
        del current_index_data[entry_key]
        
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=full_index_s3_key,
                Body=json.dumps(current_index_data, indent=2),
                ContentType='application/json'
            )
            print(f"Successfully removed {entry_key} from private datasets index.")
            return True
        except Exception as e:
            print(f"Error updating private datasets index after removal: {e}")
            return False
    else:
        print(f"Entry {entry_key} not found in private datasets index.")
        return True  # Consider this success since the entry doesn't exist

# --- Private Models Index Utilities ---

def _update_private_models_index(s3_client: Any, bucket_name: str, model_id: str, revision: Optional[str], config: Optional[HGLocalizationConfig] = None) -> bool:
    """Updates the private_models_index.json file in S3 when a model is uploaded privately."""
    if config is None:
        config = default_config
        
    if not s3_client: return False
    
    current_index_data = {}
    full_index_s3_key = _get_prefixed_s3_key(config.private_models_index_key, config)
    
    # Try to fetch existing index
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_index_s3_key)
        current_index_data = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"{full_index_s3_key} not found in S3, will create a new one.")
        else:
            print(f"Error fetching {full_index_s3_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {full_index_s3_key} in S3 is corrupted. Will overwrite.")
        current_index_data = {}

    # Create entry key and data
    entry_key = f"{model_id}---{revision or config.default_revision_name}"
    
    # Import here to avoid circular imports
    from .model_manager import _get_model_s3_prefix
    s3_prefix_path = _get_model_s3_prefix(model_id, revision, config)
    
    # Check what files exist for this model
    model_card_key = f"{s3_prefix_path}/model_card.md"
    model_config_key = f"{s3_prefix_path}/config.json"
    
    has_card = False
    has_config = False
    has_tokenizer = False
    is_full_model = False
    
    try:
        s3_client.head_object(Bucket=bucket_name, Key=model_card_key)
        has_card = True
    except ClientError:
        pass
    
    try:
        s3_client.head_object(Bucket=bucket_name, Key=model_config_key)
        has_config = True
    except ClientError:
        pass
    
    # Check for tokenizer files
    tokenizer_patterns = ["tokenizer.json", "tokenizer_config.json"]
    for pattern in tokenizer_patterns:
        try:
            s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix_path}/{pattern}")
            has_tokenizer = True
            break
        except ClientError:
            continue
    
    # Check for model weights (indicates full model vs metadata-only)
    weight_patterns = ["pytorch_model.bin", "model.safetensors"]
    for pattern in weight_patterns:
        try:
            s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix_path}/{pattern}")
            is_full_model = True
            break
        except ClientError:
            continue
    
    current_index_data[entry_key] = {
        "model_id": model_id,
        "revision": revision,
        "s3_prefix": s3_prefix_path,
        "s3_bucket": bucket_name,
        "has_card": has_card,
        "has_config": has_config,
        "has_tokenizer": has_tokenizer,
        "is_full_model": is_full_model,
        "last_updated": str(Path(__file__).stat().st_mtime)  # Simple timestamp
    }

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=full_index_s3_key,
            Body=json.dumps(current_index_data, indent=2),
            ContentType='application/json'
            # Note: No ACL='public-read' for private index
        )
        print(f"Successfully updated private models index {full_index_s3_key} in S3.")
        return True
    except Exception as e:
        print(f"Error uploading {full_index_s3_key} to S3: {e}")
        return False

def _fetch_private_models_index(config: Optional[HGLocalizationConfig] = None) -> Optional[Dict[str, Any]]:
    """Fetches the private_models_index.json from S3 using authenticated access."""
    if config is None:
        config = default_config
        
    s3_client = _get_s3_client(config)
    if not s3_client or not config.s3_bucket_name:
        print("S3 client not available or bucket not configured. Cannot fetch private models index.")
        return None

    full_index_s3_key = _get_prefixed_s3_key(config.private_models_index_key, config)
    
    try:
        print(f"Fetching private models index from: s3://{config.s3_bucket_name}/{full_index_s3_key}")
        response = s3_client.get_object(Bucket=config.s3_bucket_name, Key=full_index_s3_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Private models index not found at s3://{config.s3_bucket_name}/{full_index_s3_key}")
        else:
            print(f"Error fetching private models index: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Private models index at s3://{config.s3_bucket_name}/{full_index_s3_key} is not valid JSON.")
        return None
    except Exception as e:
        print(f"Unexpected error fetching private models index: {e}")
        return None

def _remove_from_private_models_index(s3_client: Any, bucket_name: str, model_id: str, revision: Optional[str], config: Optional[HGLocalizationConfig] = None) -> bool:
    """Removes a model entry from the private_models_index.json file in S3."""
    if config is None:
        config = default_config
        
    if not s3_client: return False
    
    full_index_s3_key = _get_prefixed_s3_key(config.private_models_index_key, config)
    
    # Try to fetch existing index
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_index_s3_key)
        current_index_data = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Private models index {full_index_s3_key} not found, nothing to remove.")
            return True  # Consider this success since the entry doesn't exist anyway
        else:
            print(f"Error fetching {full_index_s3_key} from S3: {e}")
            return False
    except json.JSONDecodeError:
        print(f"Error: {full_index_s3_key} in S3 is corrupted.")
        return False

    # Remove entry if it exists
    entry_key = f"{model_id}---{revision or config.default_revision_name}"
    if entry_key in current_index_data:
        del current_index_data[entry_key]
        
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=full_index_s3_key,
                Body=json.dumps(current_index_data, indent=2),
                ContentType='application/json'
            )
            print(f"Successfully removed {entry_key} from private models index.")
            return True
        except Exception as e:
            print(f"Error updating private models index after removal: {e}")
            return False
    else:
        print(f"Entry {entry_key} not found in private models index.")
        return True  # Consider this success since the entry doesn't exist 