import os
import shutil
import tempfile
import json
import requests
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

from huggingface_hub import ModelCard
from datasets import load_dataset, load_from_disk, DatasetDict, Dataset
from botocore.exceptions import ClientError # For list_s3_datasets error handling

from .config import HGLocalizationConfig, default_config
from .utils import _get_safe_path_component, _restore_dataset_name, _zip_directory, _unzip_file
from .s3_utils import (
    _get_s3_client, _get_s3_prefix, _get_prefixed_s3_key,
    _check_s3_dataset_exists, _upload_directory_to_s3,
    _download_directory_from_s3, _update_public_datasets_json,
    _get_s3_public_url, get_s3_dataset_card_presigned_url
)

# --- Path Utilities specific to dataset_manager ---

def _get_dataset_path(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> Path:
    """Constructs the local storage path for a dataset version.
    
    Now includes bucket information to prevent path collisions when the same dataset
    is downloaded from different S3 buckets/endpoints.
    """
    if config is None:
        config = default_config
        
    safe_dataset_id = _get_safe_path_component(dataset_id)
    safe_config_name = _get_safe_path_component(config_name if config_name else config.default_config_name)
    safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
    
    # Use public store path if this is a public dataset
    base_path = config.public_datasets_store_path if is_public else config.datasets_store_path
    
    # For bucket-specific storage, include bucket information in the path
    # This prevents collisions when the same dataset is downloaded from different buckets
    # Apply to both public and private datasets when bucket is configured
    if config.s3_bucket_name:
        # Create a safe bucket identifier
        safe_bucket_name = _get_safe_path_component(config.s3_bucket_name)
        # Include endpoint URL hash if present to distinguish between different S3-compatible services
        bucket_identifier = safe_bucket_name
        if config.s3_endpoint_url:
            import hashlib
            endpoint_hash = hashlib.md5(config.s3_endpoint_url.encode()).hexdigest()[:8]
            bucket_identifier = f"{safe_bucket_name}_{endpoint_hash}"
        
        return base_path / "by_bucket" / bucket_identifier / safe_dataset_id / safe_config_name / safe_revision
    else:
        # When no bucket is configured, use the original path structure
        return base_path / safe_dataset_id / safe_config_name / safe_revision

def _get_dataset_bucket_metadata_path(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> Path:
    """Get the path to the bucket metadata file for a dataset."""
    dataset_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public)
    return dataset_path / ".hg_localization_bucket_metadata.json"

def _store_dataset_bucket_metadata(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> None:
    """Store metadata about which S3 bucket/endpoint this dataset came from."""
    if config is None:
        config = default_config
    
    metadata_path = _get_dataset_bucket_metadata_path(dataset_id, config_name, revision, config, is_public)
    
    # Create metadata about the source bucket
    metadata = {
        "s3_bucket_name": config.s3_bucket_name,
        "s3_endpoint_url": config.s3_endpoint_url,
        "s3_data_prefix": config.s3_data_prefix,
        "cached_timestamp": json.dumps({"timestamp": str(Path(__file__).stat().st_mtime)}),  # Simple timestamp
        "is_public": is_public
    }
    
    try:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        print(f"Stored bucket metadata for dataset {dataset_id} at {metadata_path}")
    except Exception as e:
        print(f"Warning: Failed to store bucket metadata for {dataset_id}: {e}")

def _get_dataset_bucket_metadata(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> Optional[Dict[str, Any]]:
    """Retrieve metadata about which S3 bucket/endpoint this dataset came from."""
    if config is None:
        config = default_config
    
    metadata_path = _get_dataset_bucket_metadata_path(dataset_id, config_name, revision, config, is_public)
    
    if not metadata_path.exists():
        return None
    
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Failed to read bucket metadata for {dataset_id}: {e}")
        return None

def _dataset_matches_current_bucket(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> bool:
    """Check if a cached dataset matches the current S3 bucket configuration."""
    if config is None:
        config = default_config
    
    metadata = _get_dataset_bucket_metadata(dataset_id, config_name, revision, config, is_public)
    
    # If no metadata exists, this is an old dataset - include it for backward compatibility
    # but only if no S3 bucket is currently configured (local-only mode)
    if metadata is None:
        return config.s3_bucket_name is None
    
    # If no S3 bucket is currently configured, only show datasets that also have no bucket metadata
    if config.s3_bucket_name is None:
        return metadata.get("s3_bucket_name") is None
    
    # Compare bucket configuration
    return (
        metadata.get("s3_bucket_name") == config.s3_bucket_name and
        metadata.get("s3_endpoint_url") == config.s3_endpoint_url and
        metadata.get("s3_data_prefix") == config.s3_data_prefix
    )

# --- Public Datasets Manifest Fetching (via URL) ---

def _fetch_public_datasets_json_via_url(config: Optional[HGLocalizationConfig] = None) -> Optional[Dict[str, Any]]:
    """Fetches the public_datasets.json via direct HTTPS GET if config.s3_bucket_name is set."""
    if config is None:
        config = default_config
        
    if not config.s3_bucket_name:
        print("config.s3_bucket_name not configured. Cannot fetch public datasets JSON.")
        return None

    prefixed_json_key = _get_prefixed_s3_key(config.public_datasets_json_key, config)
    json_url = _get_s3_public_url(config.s3_bucket_name, prefixed_json_key, config.s3_endpoint_url)
    print(f"Attempting to fetch public datasets JSON from: {json_url}")
    try:
        response = requests.get(json_url, timeout=10)
        response.raise_for_status()
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

def _fetch_public_dataset_info(dataset_id: str, config_name: Optional[str], revision: Optional[str], config: Optional[HGLocalizationConfig] = None) -> Optional[Dict[str, str]]:
    """Fetches a specific dataset's public zip info from public_datasets.json using direct URL access."""
    if config is None:
        config = default_config
        
    public_config = _fetch_public_datasets_json_via_url(config)
    if not public_config:
        return None
        
    entry_key = f"{dataset_id}---{config_name or config.default_config_name}---{revision or config.default_revision_name}"
    dataset_info = public_config.get(entry_key)
    if dataset_info:
        if not all(k in dataset_info for k in ["s3_zip_key", "s3_bucket"]):
            print(f"Error: Public dataset info for {entry_key} is incomplete. Missing 's3_zip_key' or 's3_bucket'.")
            return None
        print(f"Found public dataset info for {entry_key}: {dataset_info}")
        return dataset_info
    else:
        print(f"Public dataset info not found for {entry_key} in {config.public_datasets_json_key}")
        return None

# --- Dataset Card Utilities ---

def get_dataset_card_url(dataset_id: str) -> str:
    """Constructs the URL to the dataset's card on the Hugging Face Hub."""
    return f"https://huggingface.co/datasets/{dataset_id}"

def get_dataset_card_content(dataset_id: str, revision: Optional[str] = None) -> Optional[str]:
    """Fetches the Markdown content of a dataset card from the Hugging Face Hub."""
    try:
        print(f"Attempting to load dataset card for: {dataset_id} (revision: {revision or 'main'})")
        # Try with revision first, fall back to without revision if not supported
        try:
            card = ModelCard.load(dataset_id, repo_type="dataset", revision=revision)
        except TypeError:
            # revision parameter not supported in this version of huggingface_hub
            print(f"Revision parameter not supported, loading default version for {dataset_id}")
            card = ModelCard.load(dataset_id, repo_type="dataset")
        return card.text
    except Exception as e:
        print(f"Error loading dataset card for '{dataset_id}' (revision: {revision or 'main'}): {e}")
        return None

def get_cached_dataset_card_content(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None) -> Optional[str]:
    """Retrieves dataset card content, prioritizing local cache, then private S3."""
    if config is None:
        config = default_config
        
    local_dataset_dir = _get_dataset_path(dataset_id, config_name, revision, config)
    local_card_file_path = local_dataset_dir / "dataset_card.md"
    version_str = f"(dataset: {dataset_id}, config: {config_name or 'default'}, revision: {revision or 'default'})"

    if local_card_file_path.exists() and local_card_file_path.is_file():
        print(f"Found dataset card locally for {version_str} at {local_card_file_path}")
        try:
            with open(local_card_file_path, "r", encoding="utf-8") as f:
                return f.read()
        except IOError as e:
            print(f"Error reading local dataset card {local_card_file_path}: {e}")

    print(f"Dataset card not found or readable locally for {version_str}. Checking S3 (private path)...")
    s3_client = _get_s3_client(config)
    if s3_client and config.s3_bucket_name:
        s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision, config) # This now comes from s3_utils and includes S3_DATA_PREFIX
        s3_card_key = f"{s3_prefix_path.rstrip('/')}/dataset_card.md"
        
        try:
            print(f"Attempting to download dataset card from S3: s3://{config.s3_bucket_name}/{s3_card_key}")
            os.makedirs(local_dataset_dir, exist_ok=True)
            s3_client.download_file(config.s3_bucket_name, s3_card_key, str(local_card_file_path))
            print(f"Successfully downloaded dataset card from S3 to {local_card_file_path}")
            with open(local_card_file_path, "r", encoding="utf-8") as f:
                return f.read()
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                print(f"Dataset card not found on S3 at {s3_card_key}")
            else:
                print(f"S3 ClientError when trying to download dataset card {s3_card_key}: {e}")
        except IOError as e:
            print(f"IOError after downloading dataset card from S3 {local_card_file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error downloading/reading dataset card from S3 {s3_card_key}: {e}")
    else:
        print("S3 client not available or bucket not configured. Cannot fetch dataset card from S3.")
    return None

# --- Core Public API Functions ---

def download_dataset(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, trust_remote_code: bool = False, make_public: bool = False, skip_s3_upload: bool = False, config: Optional[HGLocalizationConfig] = None, force_public_cache: bool = False, skip_hf_card_fetch: bool = False) -> Tuple[bool, str]:
    if config is None:
        config = default_config
        
    version_str = f"(config: {config_name or 'default'}, revision: {revision or 'default'})"
    print(f"Processing dataset: {dataset_id} {version_str}")
    
    # Determine if this should be saved to public cache
    # This happens when make_public is True OR when force_public_cache is True (e.g., downloaded from public zip)
    save_to_public = make_public or force_public_cache
    local_save_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=save_to_public)

    # Check if the target dataset already exists in the intended location
    # When saving to public cache, we specifically check the public path
    # When saving to private cache, we check both public (preferred) and private paths
    if save_to_public:
        # For public cache, only check the public path
        if local_save_path.exists() and \
           ((local_save_path / "dataset_info.json").exists() or \
            (local_save_path / "dataset_dict.json").exists()):
            print(f"Dataset {dataset_id} {version_str} already exists in public cache at {local_save_path}")
            return True, str(local_save_path)
    else:
        # For private cache, check public path first (preferred), then private path
        public_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=True)
        private_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=False)
        
        # Check public path first
        if public_path.exists() and \
           ((public_path / "dataset_info.json").exists() or \
            (public_path / "dataset_dict.json").exists()):
            print(f"Dataset {dataset_id} {version_str} already exists in public cache at {public_path}")
            return True, str(public_path)
        
        # Then check private path
        if private_path.exists() and \
           ((private_path / "dataset_info.json").exists() or \
            (private_path / "dataset_dict.json").exists()):
            print(f"Dataset {dataset_id} {version_str} already exists in private cache at {private_path}")
            return True, str(private_path)

    s3_client = _get_s3_client(config)
    if s3_client and config.s3_bucket_name:
        s3_prefix_path_for_dataset = _get_s3_prefix(dataset_id, config_name, revision, config)
        print(f"Checking S3 for dataset {dataset_id} {version_str} at s3://{config.s3_bucket_name}/{s3_prefix_path_for_dataset}...")
        if _check_s3_dataset_exists(s3_client, config.s3_bucket_name, s3_prefix_path_for_dataset):
            print(f"Dataset found on S3. Attempting to download from S3 to local cache: {local_save_path}...")
            if _download_directory_from_s3(s3_client, local_save_path, config.s3_bucket_name, s3_prefix_path_for_dataset):
                print(f"Successfully downloaded dataset from S3 to {local_save_path}.")
                if local_save_path.exists() and \
                   ((local_save_path / "dataset_info.json").exists() or \
                    (local_save_path / "dataset_dict.json").exists()):
                    # Store bucket metadata for the downloaded dataset
                    _store_dataset_bucket_metadata(dataset_id, config_name, revision, config, is_public=save_to_public)
                    return True, str(local_save_path)
                else:
                    print(f"Error: S3 download reported success, but dataset not found or incomplete at {local_save_path}. Proceeding to Hugging Face.")
            else:
                print(f"Failed to download dataset from S3. Will attempt Hugging Face download.")
        else:
            print(f"Dataset not found on S3. Will attempt Hugging Face download.")
    else:
        print("S3 not configured or client init failed; skipping S3 check.")

    # Check for public dataset availability when S3 is not configured or dataset not found on S3
    # When saving to public cache, always check for public datasets
    # When saving to private cache, only check if no local version exists
    should_check_public = False
    if save_to_public:
        # Always check for public datasets when we want to save to public cache
        public_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=True)
        if not (public_dataset_path.exists() and 
                ((public_dataset_path / "dataset_info.json").exists() or 
                 (public_dataset_path / "dataset_dict.json").exists())):
            should_check_public = True
    else:
        # For private cache, only check public datasets if no local version exists at all
        if not (local_save_path.exists() and 
                ((local_save_path / "dataset_info.json").exists() or 
                 (local_save_path / "dataset_dict.json").exists())):
            should_check_public = True
    
    if should_check_public:
        print("Attempting to fetch from public S3 dataset list via URL...")
        public_info = _fetch_public_dataset_info(dataset_id, config_name, revision, config)
        if public_info and public_info.get('s3_zip_key') and public_info.get('s3_bucket'):
            public_s3_bucket = public_info['s3_bucket']
            # s3_zip_key from public_info is relative to S3_DATA_PREFIX (or root if no prefix)
            relative_public_s3_zip_key = public_info['s3_zip_key'] 
            # Construct full key for _get_s3_public_url
            full_public_s3_zip_key = _get_prefixed_s3_key(relative_public_s3_zip_key, config)
            
            # Determine the bucket for the URL construction
            # Usually, public_s3_bucket *is* config.s3_bucket_name, but _get_s3_public_url needs a bucket.
            # If HGLOC_S3_BUCKET_NAME is set, it's used for URL construction (especially if endpoint specific).
            # If not, use the bucket from the public JSON.
            target_bucket_for_url = config.s3_bucket_name if config.s3_bucket_name else public_s3_bucket
            if config.s3_bucket_name and public_s3_bucket != config.s3_bucket_name:
                 print(f"Warning: Public dataset info bucket '{public_s3_bucket}' differs from configured HGLOC_S3_BUCKET_NAME '{config.s3_bucket_name}'. Using '{target_bucket_for_url}' for download URL construction.")
            
            zip_url = _get_s3_public_url(target_bucket_for_url, full_public_s3_zip_key, config.s3_endpoint_url)
            print(f"Public dataset zip found. Attempting download from: {zip_url}")
            
            # Use public path for public dataset downloads
            public_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=True)
            
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_download_zip_file:
                tmp_zip_file_path = Path(tmp_download_zip_file.name)
                try:
                    response = requests.get(zip_url, stream=True, timeout=300) 
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=8192):
                        tmp_download_zip_file.write(chunk)
                    tmp_download_zip_file.close() 
                    print(f"Public zip downloaded to {tmp_zip_file_path}. Unzipping...")
                    os.makedirs(public_dataset_path, exist_ok=True) # Ensure target dir exists for unzip
                    if _unzip_file(tmp_zip_file_path, public_dataset_path):
                        print(f"Successfully downloaded and unzipped public dataset to {public_dataset_path}.")
                        # Store bucket metadata for the downloaded public dataset
                        _store_dataset_bucket_metadata(dataset_id, config_name, revision, config, is_public=True)
                        # Update local_save_path to point to the public path and return success
                        local_save_path = public_dataset_path
                        return True, str(local_save_path)
                    else:
                        print(f"Failed to unzip public dataset from {tmp_zip_file_path}.")
                        if public_dataset_path.exists(): shutil.rmtree(public_dataset_path) # Clean up partial unzip attempt
                except requests.exceptions.RequestException as e: # Catches HTTPError too
                    print(f"Error downloading public zip {zip_url}: {e}")
                    if public_dataset_path.exists(): shutil.rmtree(public_dataset_path)
                except Exception as e:
                    print(f"Unexpected error during public S3 zip download/unzip: {e}")
                    if public_dataset_path.exists(): shutil.rmtree(public_dataset_path)
                finally:
                    if not tmp_download_zip_file.closed: tmp_download_zip_file.close()
                    try:
                        os.remove(tmp_zip_file_path)
                    except OSError: pass
        else:
            print(f"Dataset '{dataset_id}' {version_str} not found in public S3 dataset list or info was incomplete.")

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
        
        # Store bucket metadata for the downloaded dataset
        _store_dataset_bucket_metadata(dataset_id, config_name, revision, config, is_public=save_to_public)

        # Try to fetch dataset card from Hugging Face (only if not skipped - e.g., when called from CLI with HF access)
        if not skip_hf_card_fetch:
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
        else:
            print(f"Skipping Hugging Face dataset card fetch for {dataset_id} {version_str} (isolated environment mode).")

        if skip_s3_upload:
            print(f"Skipping S3 upload for {dataset_id} {version_str} as requested.")
        else:
            s3_client_for_upload = _get_s3_client(config)
            if s3_client_for_upload and config.s3_bucket_name:
                s3_prefix_path_for_upload = _get_s3_prefix(dataset_id, config_name, revision, config)
                _upload_directory_to_s3(s3_client_for_upload, local_save_path, config.s3_bucket_name, s3_prefix_path_for_upload)

                if make_public:
                    print(f"Preparing to make dataset {dataset_id} {version_str} public...")
                    safe_dataset_id = _get_safe_path_component(dataset_id)
                    safe_config_name = _get_safe_path_component(config_name if config_name else config.default_config_name)
                    safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
                    
                    zip_file_name = f"{safe_dataset_id}---{safe_config_name}---{safe_revision}.zip"
                    # base_s3_zip_key is relative to S3_DATA_PREFIX (or bucket root if no prefix)
                    base_s3_zip_key = f"{config.public_datasets_zip_dir_prefix}/{zip_file_name}"
                    # s3_zip_key_full includes S3_DATA_PREFIX for the actual S3 operation
                    s3_zip_key_full = _get_prefixed_s3_key(base_s3_zip_key, config)
                    
                    with tempfile.TemporaryDirectory() as tmp_zip_src_dir:
                        # Copy contents of local_save_path into a subdir of tmp_zip_src_dir to ensure correct zip structure
                        # The subdir name doesn't matter as we zip its contents.
                        dataset_content_in_temp = Path(tmp_zip_src_dir) / "dataset_content"
                        shutil.copytree(local_save_path, dataset_content_in_temp)
                        
                        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
                            tmp_zip_file_path = Path(tmp_zip_file.name)
                            if _zip_directory(dataset_content_in_temp, tmp_zip_file_path):
                                print(f"Uploading public zip {tmp_zip_file_path} to s3://{config.s3_bucket_name}/{s3_zip_key_full}")
                                try:
                                    s3_client_for_upload.upload_file(
                                        str(tmp_zip_file_path), 
                                        config.s3_bucket_name, 
                                        s3_zip_key_full,
                                        ExtraArgs={'ACL': 'public-read'}
                                    )
                                    print(f"Successfully uploaded public zip to {s3_zip_key_full}")
                                    _update_public_datasets_json(s3_client_for_upload, config.s3_bucket_name, dataset_id, config_name, revision, base_s3_zip_key, config)
                                except Exception as e:
                                    print(f"Failed to upload public zip {s3_zip_key_full}: {e}")
                            else:
                                print(f"Failed to zip dataset for public upload.")
                            try:
                                os.remove(tmp_zip_file_path)
                            except OSError: pass
        return True, str(local_save_path)

    except FileNotFoundError:
        error_msg_detail = f"Dataset '{dataset_id}'{config_str_part} not found on Hugging Face Hub."
        print(f"Error: {error_msg_detail}")
        return False, error_msg_detail
    except Exception as e:
        print(f"An error occurred while processing '{dataset_id}' {version_str}: {e}")
        if local_save_path.exists():
            try:
                shutil.rmtree(local_save_path)
                print(f"Cleaned up partially saved data at {local_save_path}")
            except Exception as cleanup_e:
                print(f"Error during cleanup of {local_save_path}: {cleanup_e}")
        return False, str(e)

def load_local_dataset(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, public_access_only: bool = False) -> Optional[DatasetDict | Dataset]:
    if config is None:
        config = default_config
        
    version_str = f"(config: {config_name or config.default_config_name.replace('_',' ')}, revision: {revision or config.default_revision_name.replace('_',' ')})"
    
    # Determine which paths to check based on access mode
    if public_access_only:
        # Public access only - check public path only
        paths_to_check = [_get_dataset_path(dataset_id, config_name, revision, config, is_public=True)]
        print(f"Public access mode: checking public cache only for '{dataset_id}' {version_str}")
    else:
        # Private access - check public path first (preferred), then private path
        public_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=True)
        private_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=False)
        paths_to_check = [public_path, private_path]
        print(f"Private access mode: checking both public and private cache for '{dataset_id}' {version_str}")
    
    # Try to find the dataset in the available paths
    local_dataset_path = None
    for path in paths_to_check:
        if (path.exists() and 
            ((path / "dataset_info.json").exists() or 
             (path / "dataset_dict.json").exists())):
            local_dataset_path = path
            print(f"Found dataset at: {local_dataset_path}")
            break
    
    if local_dataset_path is None:
        print(f"Dataset '{dataset_id}' {version_str} not found in local cache.")
        
        if config.aws_access_key_id and config.aws_secret_access_key:
            s3_client_auth = _get_s3_client(config)
            if s3_client_auth and config.s3_bucket_name:
                print("Attempting to fetch from S3 using credentials...")
                s3_prefix_path_for_download = _get_s3_prefix(dataset_id, config_name, revision, config)
                # Use private path for authenticated S3 downloads
                private_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=False)
                if _download_directory_from_s3(s3_client_auth, private_dataset_path, config.s3_bucket_name, s3_prefix_path_for_download):
                    print(f"Successfully downloaded from S3 (authenticated) to {private_dataset_path}.")
                    local_dataset_path = private_dataset_path
                else:
                    print(f"Failed to download '{dataset_id}' {version_str} from S3 (authenticated) or not found.")
            else: 
                print("S3 client could not be initialized for authenticated download.")
        
        if not (local_dataset_path and local_dataset_path.exists() and \
                ((local_dataset_path / "dataset_info.json").exists() or \
                 (local_dataset_path / "dataset_dict.json").exists())):
            print("Attempting to fetch from public S3 dataset list via URL...")
            public_info = _fetch_public_dataset_info(dataset_id, config_name, revision, config)
            if public_info and public_info.get('s3_zip_key') and public_info.get('s3_bucket'):
                public_s3_bucket = public_info['s3_bucket']
                # s3_zip_key from public_info is relative to S3_DATA_PREFIX (or root if no prefix)
                relative_public_s3_zip_key = public_info['s3_zip_key'] 
                # Construct full key for _get_s3_public_url
                full_public_s3_zip_key = _get_prefixed_s3_key(relative_public_s3_zip_key, config)
                
                # Determine the bucket for the URL construction
                # Usually, public_s3_bucket *is* config.s3_bucket_name, but _get_s3_public_url needs a bucket.
                # If HGLOC_S3_BUCKET_NAME is set, it's used for URL construction (especially if endpoint specific).
                # If not, use the bucket from the public JSON.
                target_bucket_for_url = config.s3_bucket_name if config.s3_bucket_name else public_s3_bucket
                if config.s3_bucket_name and public_s3_bucket != config.s3_bucket_name:
                     print(f"Warning: Public dataset info bucket '{public_s3_bucket}' differs from configured HGLOC_S3_BUCKET_NAME '{config.s3_bucket_name}'. Using '{target_bucket_for_url}' for download URL construction.")
                
                zip_url = _get_s3_public_url(target_bucket_for_url, full_public_s3_zip_key, config.s3_endpoint_url)
                print(f"Public dataset zip found. Attempting download from: {zip_url}")
                
                # Use public path for public dataset downloads
                public_dataset_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=True)
                
                with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_download_zip_file:
                    tmp_zip_file_path = Path(tmp_download_zip_file.name)
                    try:
                        response = requests.get(zip_url, stream=True, timeout=300) 
                        response.raise_for_status()
                        for chunk in response.iter_content(chunk_size=8192):
                            tmp_download_zip_file.write(chunk)
                        tmp_download_zip_file.close() 
                        print(f"Public zip downloaded to {tmp_zip_file_path}. Unzipping...")
                        os.makedirs(public_dataset_path, exist_ok=True) # Ensure target dir exists for unzip
                        if _unzip_file(tmp_zip_file_path, public_dataset_path):
                            print(f"Successfully downloaded and unzipped public dataset to {public_dataset_path}.")
                            # Update local_dataset_path to point to the public path for the rest of the function
                            local_dataset_path = public_dataset_path
                        else:
                            print(f"Failed to unzip public dataset from {tmp_zip_file_path}.")
                            if public_dataset_path.exists(): shutil.rmtree(public_dataset_path) # Clean up partial unzip attempt
                    except requests.exceptions.RequestException as e: # Catches HTTPError too
                        print(f"Error downloading public zip {zip_url}: {e}")
                        if public_dataset_path.exists(): shutil.rmtree(public_dataset_path)
                    except Exception as e:
                        print(f"Unexpected error during public S3 zip download/unzip: {e}")
                        if public_dataset_path.exists(): shutil.rmtree(public_dataset_path)
                    finally:
                        if not tmp_download_zip_file.closed: tmp_download_zip_file.close()
                        try:
                            os.remove(tmp_zip_file_path)
                        except OSError: pass
            else:
                 if not (local_dataset_path and local_dataset_path.exists() and \
                        ((local_dataset_path / "dataset_info.json").exists() or \
                         (local_dataset_path / "dataset_dict.json").exists())):
                    print(f"Dataset '{dataset_id}' {version_str} not found in public S3 dataset list or info was incomplete.")

        if not (local_dataset_path and local_dataset_path.exists() and 
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

def upload_dataset(dataset_obj: Dataset | DatasetDict, dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, make_public: bool = False, config: Optional[HGLocalizationConfig] = None) -> bool:
    if config is None:
        config = default_config
        
    version_str = f"(config: {config_name or 'default'}, revision: {revision or 'default'})"
    print(f"Processing dataset for upload: {dataset_id} {version_str}")
    local_save_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public=make_public)

    try:
        print(f"Saving dataset to local cache at {local_save_path}...")
        os.makedirs(local_save_path, exist_ok=True)
        dataset_obj.save_to_disk(str(local_save_path))
        print(f"Dataset '{dataset_id}' {version_str} successfully saved to local cache: {local_save_path}")
        
        # Store bucket metadata for the uploaded dataset to ensure it's properly tracked
        _store_dataset_bucket_metadata(dataset_id, config_name, revision, config, is_public=make_public)
        
        # Create a basic dataset card for uploaded datasets
        card_file_path = local_save_path / "dataset_card.md"
        if not card_file_path.exists():
            try:
                card_content = f"""# {dataset_id}

## Dataset Description

This dataset was uploaded locally using the HG-Localization library.

**Configuration:** {config_name or 'default'}  
**Revision:** {revision or 'default'}  
**Upload Type:** {'Public' if make_public else 'Private'}

## Dataset Structure

This dataset contains the following features:
"""
                # Try to add feature information if available
                if hasattr(dataset_obj, 'features'):
                    # Single Dataset
                    for feature_name, feature_type in dataset_obj.features.items():
                        card_content += f"- **{feature_name}**: {feature_type}\n"
                elif hasattr(dataset_obj, 'column_names'):
                    # DatasetDict - use first split
                    first_split = list(dataset_obj.keys())[0]
                    for feature_name, feature_type in dataset_obj[first_split].features.items():
                        card_content += f"- **{feature_name}**: {feature_type}\n"
                
                card_content += f"""
## Usage

```python
from hg_localization import load_local_dataset

# Load the dataset
dataset = load_local_dataset(
    dataset_id='{dataset_id}',
    config_name='{config_name or 'default'}',
    revision='{revision or 'default'}'
)
```

---
*This dataset card was automatically generated by HG-Localization.*
"""
                
                with open(card_file_path, "w", encoding="utf-8") as f:
                    f.write(card_content)
                print(f"Created dataset card at {card_file_path}")
            except Exception as e:
                print(f"Warning: Failed to create dataset card at {card_file_path}: {e}")
        
    except Exception as e:
        print(f"Error saving dataset '{dataset_id}' {version_str} to {local_save_path}: {e}")
        return False

    s3_client = _get_s3_client(config)
    if s3_client and config.s3_bucket_name:
        s3_prefix_path = _get_s3_prefix(dataset_id, config_name, revision, config)
        print(f"Attempting to upload dataset from {local_save_path} to S3: s3://{config.s3_bucket_name}/{s3_prefix_path}")
        try:
            _upload_directory_to_s3(s3_client, local_save_path, config.s3_bucket_name, s3_prefix_path)
            print(f"Successfully initiated upload of dataset '{dataset_id}' {version_str} to S3.")
            
            if make_public:
                print(f"Preparing to make (uploaded) dataset {dataset_id} {version_str} public...")
                safe_dataset_id = _get_safe_path_component(dataset_id)
                safe_config_name = _get_safe_path_component(config_name if config_name else config.default_config_name)
                safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
                
                zip_file_name = f"{safe_dataset_id}---{safe_config_name}---{safe_revision}.zip"
                base_s3_zip_key = f"{config.public_datasets_zip_dir_prefix}/{zip_file_name}"
                s3_zip_key_full = _get_prefixed_s3_key(base_s3_zip_key, config)
                
                with tempfile.TemporaryDirectory() as tmp_zip_src_dir:
                    dataset_content_in_temp = Path(tmp_zip_src_dir) / "dataset_content"
                    shutil.copytree(local_save_path, dataset_content_in_temp)
                    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
                        tmp_zip_file_path = Path(tmp_zip_file.name)
                        if _zip_directory(dataset_content_in_temp, tmp_zip_file_path):
                            print(f"Uploading public zip {tmp_zip_file_path} to s3://{config.s3_bucket_name}/{s3_zip_key_full}")
                            try:
                                s3_client.upload_file(
                                    str(tmp_zip_file_path), 
                                    config.s3_bucket_name, 
                                    s3_zip_key_full,
                                    ExtraArgs={'ACL': 'public-read'}
                                )
                                print(f"Successfully uploaded public zip to {s3_zip_key_full}")
                                _update_public_datasets_json(s3_client, config.s3_bucket_name, dataset_id, config_name, revision, base_s3_zip_key, config)
                            except Exception as e:
                                print(f"Failed to upload public zip {s3_zip_key_full}: {e}")
                        else:
                            print(f"Failed to zip dataset for public upload.")
                        try:
                            os.remove(tmp_zip_file_path)
                        except OSError: pass
            return True 
        except Exception as e:
            print(f"Error uploading dataset '{dataset_id}' {version_str} to S3: {e}")
            return False 
    else: 
        print("S3 not configured or client init failed; skipping S3 upload. Dataset is saved locally.")
        if make_public:
            print("Cannot make dataset public as S3 is not configured.")
        return True # True because local save succeeded, S3 was skipped as per config

def _scan_dataset_directory(store_path: Path, config: HGLocalizationConfig, is_public_store: bool, filter_by_bucket: bool, include_legacy: bool = True) -> List[Dict[str, str]]:
    """Scan a dataset directory for both new bucket-specific and legacy storage structures."""
    datasets = []
    
    if not store_path.exists():
        return datasets
    
    # Scan new bucket-specific structure: store_path/by_bucket/bucket_id/dataset_id/config/revision
    # Apply to both public and private stores when bucket is configured
    if config.s3_bucket_name:
        by_bucket_path = store_path / "by_bucket"
        if by_bucket_path.exists():
            for bucket_dir in by_bucket_path.iterdir():
                if bucket_dir.is_dir() and not bucket_dir.name.startswith("."):
                    datasets.extend(_scan_legacy_structure(bucket_dir, config, is_public_store, filter_by_bucket))
    
    # Scan legacy structure: store_path/dataset_id/config/revision (for backward compatibility)
    if include_legacy:
        datasets.extend(_scan_legacy_structure(store_path, config, is_public_store, filter_by_bucket))
    
    return datasets

def _scan_legacy_structure(base_path: Path, config: HGLocalizationConfig, is_public_store: bool, filter_by_bucket: bool) -> List[Dict[str, str]]:
    """Scan the legacy dataset storage structure."""
    datasets = []
    
    for dataset_id_dir in base_path.iterdir():
        if dataset_id_dir.is_dir() and not dataset_id_dir.name.startswith(".") and dataset_id_dir.name != "by_bucket":
            for config_name_dir in dataset_id_dir.iterdir():
                if config_name_dir.is_dir():
                    for revision_dir in config_name_dir.iterdir():
                        if revision_dir.is_dir() and \
                           ((revision_dir / "dataset_info.json").exists() or \
                            (revision_dir / "dataset_dict.json").exists()):
                            # Convert safe names back to original format
                            dataset_id_display = _restore_dataset_name(dataset_id_dir.name)
                            config_name_display = config_name_dir.name
                            revision_display = revision_dir.name
                            
                            has_card = (revision_dir / "dataset_card.md").is_file()
                            
                            # Check if this dataset matches the current bucket configuration
                            if filter_by_bucket and not _dataset_matches_current_bucket(
                                dataset_id_display, 
                                config_name_display if config_name_display != config.default_config_name else None,
                                revision_display if revision_display != config.default_revision_name else None,
                                config, 
                                is_public=is_public_store
                            ):
                                continue  # Skip this dataset as it doesn't match current bucket
                            
                            dataset_info = {
                                "dataset_id": dataset_id_display,
                                "config_name": config_name_display if config_name_display != config.default_config_name else None,
                                "revision": revision_display if revision_display != config.default_revision_name else None,
                                "path": str(revision_dir),
                                "has_card": has_card,
                                "is_public": is_public_store
                            }
                            datasets.append(dataset_info)
    
    return datasets

def list_local_datasets(config: Optional[HGLocalizationConfig] = None, public_access_only: bool = False, filter_by_bucket: bool = True) -> List[Dict[str, str]]:
    if config is None:
        config = default_config
        
    available_datasets = []
    
    # Determine which directories to scan based on access mode
    if public_access_only:
        # Public access only - scan public directory only
        if config.public_datasets_store_path.exists():
            directories_to_scan = [(config.public_datasets_store_path, True)]
            print(f"Public access mode: scanning public datasets only")
        else:
            print(f"Local dataset store directory does not exist: {config.public_datasets_store_path}")
            return available_datasets
    else:
        # Private access - scan both public and private directories
        directories_to_scan = []
        if config.public_datasets_store_path.exists():
            directories_to_scan.append((config.public_datasets_store_path, True))
        if config.datasets_store_path.exists():
            directories_to_scan.append((config.datasets_store_path, False))
        print(f"Private access mode: scanning both public and private datasets")
        
        if not directories_to_scan:
            # Check which specific directories don't exist for better error messages
            if not config.public_datasets_store_path.exists() and not config.datasets_store_path.exists():
                print(f"Local dataset store directory does not exist: {config.datasets_store_path}")
            return available_datasets
    
    for store_path, is_public_store in directories_to_scan:
        datasets_from_store = _scan_dataset_directory(store_path, config, is_public_store, filter_by_bucket)
        
        # Merge datasets, handling duplicates (prefer public over private)
        for dataset_info in datasets_from_store:
            # Check for duplicates (same dataset in both public and private)
            # Prefer public version if it exists
            existing_idx = None
            for i, existing in enumerate(available_datasets):
                if (existing["dataset_id"] == dataset_info["dataset_id"] and 
                    existing["config_name"] == dataset_info["config_name"] and 
                    existing["revision"] == dataset_info["revision"]):
                    existing_idx = i
                    break
            
            if existing_idx is not None:
                # Dataset already exists, prefer public version
                if is_public_store:
                    available_datasets[existing_idx] = dataset_info
            else:
                available_datasets.append(dataset_info)
    
    if not available_datasets:
        print("No local datasets found in cache.")
    else:
        print(f"Found {len(available_datasets)} local dataset(s):")
        for ds_info in available_datasets:
            # Reinstating detailed print for test compatibility
            print(f"  Dataset ID: {ds_info['dataset_id']}, "
                  f"Config: {ds_info.get('config_name', config.default_config_name)}, "
                  f"Revision: {ds_info.get('revision', config.default_revision_name)}, "
                  f"Path: {ds_info['path']}, "
                  f"Card: {'Yes' if ds_info['has_card'] else 'No'}")
    return available_datasets

def list_s3_datasets(config: Optional[HGLocalizationConfig] = None) -> List[Dict[str, str]]:
    if config is None:
        config = default_config
        
    available_s3_datasets = []
    if not config.s3_bucket_name:
        print("config.s3_bucket_name not configured. Cannot list S3 datasets.")
        return []

    if config.aws_access_key_id and config.aws_secret_access_key:
        s3_client = _get_s3_client(config)
        if s3_client:
            print("Listing S3 datasets via authenticated API call (scanning bucket structure)...")
            paginator = s3_client.get_paginator('list_objects_v2')
            
            # scan_base_prefix should be the S3_DATA_PREFIX, ensuring it ends with a slash if not empty
            scan_base_prefix = config.s3_data_prefix.strip('/') + '/' if config.s3_data_prefix else ""

            try:
                # Iterate through dataset_id level
                for page1 in paginator.paginate(Bucket=config.s3_bucket_name, Prefix=scan_base_prefix, Delimiter='/'):
                    for common_prefix1 in page1.get('CommonPrefixes', []): # These are "directories" at dataset_id level
                        dataset_id_full_prefix = common_prefix1.get('Prefix', '') # e.g., "my_data_prefix/dataset_id_safe/"
                        if not dataset_id_full_prefix or not dataset_id_full_prefix.endswith('/'): continue
                        
                        # Extract dataset_id part relative to scan_base_prefix
                        dataset_id_from_s3 = dataset_id_full_prefix[len(scan_base_prefix):].strip('/')
                        if not dataset_id_from_s3: continue # Skip if it's somehow empty

                        # Iterate through config_name level
                        for page2 in paginator.paginate(Bucket=config.s3_bucket_name, Prefix=dataset_id_full_prefix, Delimiter='/'):
                            for common_prefix2 in page2.get('CommonPrefixes', []):
                                config_name_full_prefix = common_prefix2.get('Prefix', '') # e.g., "my_data_prefix/dataset_id_safe/config_name_safe/"
                                if not config_name_full_prefix or not config_name_full_prefix.endswith('/'): continue
                                
                                config_name_from_s3 = config_name_full_prefix[len(dataset_id_full_prefix):].strip('/')
                                if not config_name_from_s3: continue

                                # Iterate through revision level
                                for page3 in paginator.paginate(Bucket=config.s3_bucket_name, Prefix=config_name_full_prefix, Delimiter='/'):
                                    for common_prefix3 in page3.get('CommonPrefixes', []):
                                        revision_full_prefix = common_prefix3.get('Prefix', '') # e.g., "my_data_prefix/dataset_id_safe/config_name_safe/revision_safe/"
                                        if not revision_full_prefix or not revision_full_prefix.endswith('/'): continue
                                        
                                        revision_from_s3 = revision_full_prefix[len(config_name_full_prefix):].strip('/')
                                        if not revision_from_s3: continue
                                        
                                        # The revision_full_prefix is the s3_prefix for this specific dataset version
                                        current_dataset_version_s3_prefix = revision_full_prefix.rstrip('/')
                                        
                                        if _check_s3_dataset_exists(s3_client, config.s3_bucket_name, current_dataset_version_s3_prefix):
                                            # Note: dataset_id, config_name, revision are the "safe" names from S3 path
                                            # The CLI or user might expect original names if they were different.
                                            # For consistency, we list what's in S3 path structure.
                                            s3_card_url = get_s3_dataset_card_presigned_url(
                                                dataset_id=dataset_id_from_s3, # Use the actual path components
                                                config_name=config_name_from_s3,
                                                revision=revision_from_s3,
                                                config=config
                                            )
                                            available_s3_datasets.append({
                                                "dataset_id": _restore_dataset_name(dataset_id_from_s3),
                                                "config_name": config_name_from_s3 if config_name_from_s3 != config.default_config_name else None,
                                                "revision": revision_from_s3 if revision_from_s3 != config.default_revision_name else None,
                                                "s3_card_url": s3_card_url
                                            })
                if available_s3_datasets:
                    print("Successfully listed S3 datasets by scanning bucket structure.")
                    return available_s3_datasets
                print("No datasets found by scanning S3 bucket structure (or structure did not match expected format).")
            except ClientError as e:
                print(f"Error listing S3 datasets via API: {e}. Falling back to check public list if applicable.")
        else:
            print("S3 client could not be initialized despite credentials appearing to be set. Proceeding to check public list.")

    # Fallback or primary method if no AWS creds: list from public_datasets.json
    # Only do this if we haven't populated available_s3_datasets from scanning yet.
    if not available_s3_datasets:
        print("Attempting to list S3 datasets from public_datasets.json...")
        public_json_content = _fetch_public_datasets_json_via_url(config)
        if public_json_content:
            public_datasets_from_json = []
            for entry_key, entry_data in public_json_content.items():
                if isinstance(entry_data, dict) and all(k in entry_data for k in ["dataset_id", "s3_zip_key"]):
                    # For public list, card URL isn't directly available this way without another S3 call.
                    # We could generate a public URL to the card if we knew its exact key and it was public.
                    public_datasets_from_json.append({
                        "dataset_id": entry_data.get("dataset_id"),  # This should already be in original format from public JSON
                        "config_name": entry_data.get("config_name"), # Already None if not present or was default
                        "revision": entry_data.get("revision"),     # Already None if not present or was default
                        "s3_card_url": None # Card URL not available directly from public JSON manifest listing
                    })
                else:
                    print(f"Skipping malformed entry in public_datasets.json: {entry_key}")
            
            if public_datasets_from_json:
                print("Listing S3 datasets based on public_datasets.json.")
                available_s3_datasets.extend(public_datasets_from_json) # Extend, in case authenticated scan found some but errored before completing
            else:
                print(f"No datasets found or listed in {config.public_datasets_json_key} at public URL (or it was empty).")
        else:
            print(f"Could not fetch or parse {config.public_datasets_json_key} from public URL.")
    
    if not available_s3_datasets:
        print(f"No datasets found in S3 bucket '{config.s3_bucket_name}' by scanning or from public list.")
    return available_s3_datasets


# --- Sync Local to S3 Functions ---

def sync_local_dataset_to_s3(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, make_public: bool = False, config: Optional[HGLocalizationConfig] = None) -> Tuple[bool, str]:
    if config is None:
        config = default_config
        
    version_str = f"(config: {config_name or 'default'}, revision: {revision or 'default'})"
    print(f"Attempting to sync local dataset to S3: {dataset_id} {version_str}")

    local_save_path = _get_dataset_path(dataset_id, config_name, revision, config)

    if not (local_save_path.exists() and \
            ((local_save_path / "dataset_info.json").exists() or \
             (local_save_path / "dataset_dict.json").exists())):
        msg = f"Local dataset {dataset_id} {version_str} not found or is incomplete at {local_save_path}. Cannot sync."
        print(msg)
        return False, msg

    s3_client = _get_s3_client(config)
    if not s3_client or not config.s3_bucket_name:
        msg = "S3 not configured (bucket name or client init failed). Cannot sync to S3."
        if make_public: msg += " Cannot make dataset public."
        print(msg)
        return False, msg

    # s3_prefix_path_for_dataset includes the S3_DATA_PREFIX
    s3_prefix_path_for_dataset = _get_s3_prefix(dataset_id, config_name, revision, config)
    private_s3_copy_exists = _check_s3_dataset_exists(s3_client, config.s3_bucket_name, s3_prefix_path_for_dataset)

    if private_s3_copy_exists:
        print(f"Dataset {dataset_id} {version_str} already exists as private S3 copy at s3://{config.s3_bucket_name}/{s3_prefix_path_for_dataset}.")
    else:
        print(f"Dataset {dataset_id} {version_str} not found on S3 (private). Uploading from {local_save_path} to s3://{config.s3_bucket_name}/{s3_prefix_path_for_dataset}")
        try:
            _upload_directory_to_s3(s3_client, local_save_path, config.s3_bucket_name, s3_prefix_path_for_dataset)
            print(f"Successfully uploaded dataset '{dataset_id}' {version_str} to S3 (private).")
            private_s3_copy_exists = True
        except Exception as e:
            msg = f"Error uploading dataset '{dataset_id}' {version_str} to S3 (private): {e}"
            print(msg)
            return False, msg

    if make_public and private_s3_copy_exists:
        print(f"Processing --make-public for dataset {dataset_id} {version_str}...")
        safe_dataset_id = _get_safe_path_component(dataset_id)
        safe_config_name = _get_safe_path_component(config_name if config_name else config.default_config_name)
        safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
        
        zip_file_name = f"{safe_dataset_id}---{safe_config_name}---{safe_revision}.zip"
        # base_s3_zip_key is relative to S3_DATA_PREFIX (or bucket root if no prefix)
        base_s3_zip_key = f"{config.public_datasets_zip_dir_prefix}/{zip_file_name}"
        # s3_zip_key_full includes S3_DATA_PREFIX for the actual S3 operation
        s3_zip_key_full = _get_prefixed_s3_key(base_s3_zip_key, config)
        
        public_zip_uploaded_or_existed = False
        try:
            s3_client.head_object(Bucket=config.s3_bucket_name, Key=s3_zip_key_full)
            print(f"Public zip s3://{config.s3_bucket_name}/{s3_zip_key_full} already exists.")
            public_zip_uploaded_or_existed = True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Public zip s3://{config.s3_bucket_name}/{s3_zip_key_full} not found. Will attempt to create and upload from {local_save_path}.")
                with tempfile.TemporaryDirectory() as tmp_zip_src_dir:
                    dataset_content_in_temp = Path(tmp_zip_src_dir) / "dataset_content_for_public_sync_zip"
                    shutil.copytree(local_save_path, dataset_content_in_temp) # Zip from a clean copy
                    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip_file:
                        tmp_zip_file_path = Path(tmp_zip_file.name)
                        zip_creation_upload_success = False
                        try:
                            if _zip_directory(dataset_content_in_temp, tmp_zip_file_path): # Zip the copied content
                                print(f"Uploading public zip {tmp_zip_file_path.name} to s3://{config.s3_bucket_name}/{s3_zip_key_full}")
                                s3_client.upload_file(
                                    str(tmp_zip_file_path), config.s3_bucket_name, s3_zip_key_full,
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
                            try: os.remove(tmp_zip_file_path) 
                            except OSError: pass
                        if not zip_creation_upload_success:
                             print(f"Skipping manifest update for {dataset_id} {version_str} due to zip creation/upload failure.")
            else:
                print(f"Error checking for existing public zip {s3_zip_key_full}: {e}. Skipping make_public actions.")

        if public_zip_uploaded_or_existed:
            print(f"Updating public_datasets.json for {dataset_id} {version_str} with zip key {base_s3_zip_key}")
            if not _update_public_datasets_json(s3_client, config.s3_bucket_name, dataset_id, config_name, revision, base_s3_zip_key, config):
                print(f"Warning: Failed to update public datasets JSON for {dataset_id} {version_str}, though public zip should exist at {s3_zip_key_full}.")
    elif make_public and not private_s3_copy_exists:
        print(f"Cannot make {dataset_id} {version_str} public because its private S3 copy does not exist or failed to upload.")

    final_msg = f"Sync process for {dataset_id} {version_str} completed."
    # ... (rest of message construction can remain similar)
    print(final_msg)
    return True, final_msg

def sync_all_local_to_s3(make_public: bool = False, config: Optional[HGLocalizationConfig] = None) -> None:
    """Iterates through all local datasets and attempts to sync them to S3."""
    if config is None:
        config = default_config
        
    print(f"Starting sync of all local datasets to S3. Make public: {make_public}")
    # Use filter_by_bucket=False to sync all local datasets regardless of bucket configuration
    local_datasets = list_local_datasets(config, filter_by_bucket=False)
    if not local_datasets:
        print("No local datasets found in cache to sync.")
        return

    succeeded_syncs = 0
    failed_syncs = 0
    
    s3_client_check = _get_s3_client(config) # Check once if S3 is usable
    if not s3_client_check or not config.s3_bucket_name:
        print("S3 not configured (bucket name or client init failed). Cannot sync any datasets to S3.")
        if make_public: print("Cannot make datasets public.")
        return

    for ds_info in local_datasets:
        dataset_id = ds_info['dataset_id']
        config_name = ds_info.get('config_name') 
        revision = ds_info.get('revision')     
        
        print(f"\n--- Processing local dataset for sync: ID='{dataset_id}', Config='{config_name}', Revision='{revision}' ---")
        # sync_local_dataset_to_s3 will use its own S3 client instance
        success, message = sync_local_dataset_to_s3(dataset_id, config_name, revision, make_public=make_public, config=config)
        if success:
            succeeded_syncs += 1
        else:
            failed_syncs += 1
            
    print("\n--- Sync all local datasets to S3 finished ---")
    print(f"Total local datasets processed: {len(local_datasets)}")
    print(f"Successfully processed (primary sync action): {succeeded_syncs}")
    print(f"Failed to process (see logs for errors): {failed_syncs}") 

def _get_legacy_dataset_path(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> Path:
    """Get the old dataset path structure (before bucket-specific storage)."""
    if config is None:
        config = default_config
        
    safe_dataset_id = _get_safe_path_component(dataset_id)
    safe_config_name = _get_safe_path_component(config_name if config_name else config.default_config_name)
    safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
    
    base_path = config.public_datasets_store_path if is_public else config.datasets_store_path
    return base_path / safe_dataset_id / safe_config_name / safe_revision

def migrate_dataset_to_bucket_storage(dataset_id: str, config_name: Optional[str] = None, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> bool:
    """Migrate a dataset from legacy storage to bucket-specific storage.
    
    Returns True if migration was successful or not needed, False if failed.
    """
    if config is None:
        config = default_config
    
    # Skip migration when no bucket is configured
    if not config.s3_bucket_name:
        return True
    
    legacy_path = _get_legacy_dataset_path(dataset_id, config_name, revision, config, is_public)
    new_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public)
    
    # If legacy path doesn't exist, no migration needed
    if not legacy_path.exists():
        return True
    
    # If new path already exists, don't overwrite
    if new_path.exists():
        print(f"Warning: New bucket-specific path already exists for {dataset_id}, skipping migration")
        return True
    
    # Check if this is a valid dataset directory
    if not ((legacy_path / "dataset_info.json").exists() or (legacy_path / "dataset_dict.json").exists()):
        print(f"Warning: {legacy_path} doesn't appear to be a valid dataset directory, skipping migration")
        return True
    
    try:
        # Create parent directories for new path
        new_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Move the dataset directory
        shutil.move(str(legacy_path), str(new_path))
        
        # Store bucket metadata for the migrated dataset
        _store_dataset_bucket_metadata(dataset_id, config_name, revision, config, is_public)
        
        print(f"Successfully migrated dataset {dataset_id} from {legacy_path} to {new_path}")
        return True
        
    except Exception as e:
        print(f"Error migrating dataset {dataset_id}: {e}")
        return False

def migrate_all_datasets_to_bucket_storage(config: Optional[HGLocalizationConfig] = None) -> None:
    """Migrate all existing datasets to bucket-specific storage structure."""
    if config is None:
        config = default_config
    
    if not config.s3_bucket_name:
        print("No S3 bucket configured, skipping migration")
        return
    
    print("Starting migration of existing datasets to bucket-specific storage...")
    
    # Get all datasets using the old listing method (without bucket filtering)
    old_datasets = list_local_datasets(config=config, filter_by_bucket=False)
    
    migrated_count = 0
    failed_count = 0
    
    for dataset_info in old_datasets:
        dataset_id = dataset_info["dataset_id"]
        config_name = dataset_info.get("config_name")
        revision = dataset_info.get("revision")
        is_public = dataset_info.get("is_public", False)
        
        if migrate_dataset_to_bucket_storage(dataset_id, config_name, revision, config, is_public):
            migrated_count += 1
        else:
            failed_count += 1
    
    print(f"Migration completed: {migrated_count} datasets migrated, {failed_count} failed") 