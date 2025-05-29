import os
import shutil
import tempfile
import json
import requests
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

from huggingface_hub import ModelCard, hf_hub_download
from botocore.exceptions import ClientError

from .config import HGLocalizationConfig, default_config
from .utils import _get_safe_path_component, _restore_dataset_name, _zip_directory, _unzip_file
from .s3_utils import (
    _get_s3_client, _get_s3_prefix, _get_prefixed_s3_key,
    _upload_directory_to_s3, _download_directory_from_s3,
    _get_s3_public_url, _update_public_models_json, _make_model_metadata_public,
    _update_private_models_index, _fetch_private_models_index
)

# --- Path Utilities specific to model_manager ---

def _get_model_path(model_id: str, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> Path:
    """Constructs the local storage path for a model version.
    
    Similar to dataset paths but for models.
    """
    if config is None:
        config = default_config
        
    safe_model_id = _get_safe_path_component(model_id)
    safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
    
    # Use public store path if this is a public model
    base_path = config.public_models_store_path if is_public else config.models_store_path
    
    # For bucket-specific storage, include bucket information in the path
    if config.s3_bucket_name:
        # Create a safe bucket identifier
        safe_bucket_name = _get_safe_path_component(config.s3_bucket_name)
        # Include endpoint URL hash if present to distinguish between different S3-compatible services
        bucket_identifier = safe_bucket_name
        if config.s3_endpoint_url:
            import hashlib
            endpoint_hash = hashlib.md5(config.s3_endpoint_url.encode()).hexdigest()[:8]
            bucket_identifier = f"{safe_bucket_name}_{endpoint_hash}"
        
        return base_path / "by_bucket" / bucket_identifier / safe_model_id / safe_revision
    else:
        # When no bucket is configured, use the original path structure
        return base_path / safe_model_id / safe_revision

def _get_model_s3_prefix(model_id: str, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None) -> str:
    """Get the S3 prefix for a model, including the data prefix."""
    if config is None:
        config = default_config
        
    safe_model_id = _get_safe_path_component(model_id)
    safe_revision = _get_safe_path_component(revision if revision else config.default_revision_name)
    
    # Use models prefix instead of datasets
    model_prefix = f"models/{safe_model_id}/{safe_revision}"
    
    if config.s3_data_prefix:
        return f"{config.s3_data_prefix.rstrip('/')}/{model_prefix}"
    return model_prefix

def _store_model_bucket_metadata(model_id: str, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None, is_public: bool = False) -> None:
    """Store metadata about which S3 bucket/endpoint this model came from."""
    if config is None:
        config = default_config
    
    model_path = _get_model_path(model_id, revision, config, is_public)
    metadata_path = model_path / ".hg_localization_bucket_metadata.json"
    
    # Create metadata about the source bucket
    metadata = {
        "s3_bucket_name": config.s3_bucket_name,
        "s3_endpoint_url": config.s3_endpoint_url,
        "s3_data_prefix": config.s3_data_prefix,
        "cached_timestamp": str(Path(__file__).stat().st_mtime),
        "is_public": is_public,
        "type": "model"  # Distinguish from dataset metadata
    }
    
    try:
        os.makedirs(metadata_path.parent, exist_ok=True)
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        print(f"Stored bucket metadata for model {model_id} at {metadata_path}")
    except Exception as e:
        print(f"Warning: Failed to store bucket metadata for {model_id}: {e}")

# --- Model Card Utilities ---

def get_model_card_url(model_id: str) -> str:
    """Constructs the URL to the model's card on the Hugging Face Hub."""
    return f"https://huggingface.co/{model_id}"

def get_model_card_content(model_id: str, revision: Optional[str] = None) -> Optional[str]:
    """Fetches the Markdown content of a model card from the Hugging Face Hub."""
    try:
        print(f"Attempting to load model card for: {model_id} (revision: {revision or 'main'})")
        # Try with revision first, fall back to without revision if not supported
        try:
            card = ModelCard.load(model_id, revision=revision)
        except TypeError:
            # revision parameter not supported in this version of huggingface_hub
            print(f"Revision parameter not supported, loading default version for {model_id}")
            card = ModelCard.load(model_id)
        return card.text
    except Exception as e:
        print(f"Error loading model card for '{model_id}' (revision: {revision or 'main'}): {e}")
        return None

def get_model_config_content(model_id: str, revision: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Fetches the config.json content from the Hugging Face Hub."""
    try:
        print(f"Attempting to load model config for: {model_id} (revision: {revision or 'main'})")
        config_path = hf_hub_download(
            repo_id=model_id,
            filename="config.json",
            revision=revision,
            cache_dir=None  # Don't use HF cache, we'll manage our own
        )
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading model config for '{model_id}' (revision: {revision or 'main'}): {e}")
        return None

def get_cached_model_card_content(model_id: str, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None) -> Optional[str]:
    """Retrieves model card content, prioritizing local cache (public then private), then private S3, then public URLs."""
    if config is None:
        config = default_config
        
    version_str = f"(model: {model_id}, revision: {revision or 'default'})"
    
    # Check public cache first (preferred)
    public_model_dir = _get_model_path(model_id, revision, config, is_public=True)
    public_card_file_path = public_model_dir / "model_card.md"
    
    if public_card_file_path.exists() and public_card_file_path.is_file():
        print(f"Found model card in public cache for {version_str} at {public_card_file_path}")
        try:
            with open(public_card_file_path, "r", encoding="utf-8") as f:
                return f.read()
        except IOError as e:
            print(f"Error reading public model card {public_card_file_path}: {e}")
    
    # Check private cache second
    private_model_dir = _get_model_path(model_id, revision, config, is_public=False)
    private_card_file_path = private_model_dir / "model_card.md"
    
    if private_card_file_path.exists() and private_card_file_path.is_file():
        print(f"Found model card in private cache for {version_str} at {private_card_file_path}")
        try:
            with open(private_card_file_path, "r", encoding="utf-8") as f:
                return f.read()
        except IOError as e:
            print(f"Error reading private model card {private_card_file_path}: {e}")

    print(f"Model card not found locally for {version_str}. Checking S3 (private path)...")
    s3_client = _get_s3_client(config)
    if s3_client and config.s3_bucket_name:
        s3_prefix_path = _get_model_s3_prefix(model_id, revision, config)
        s3_card_key = f"{s3_prefix_path.rstrip('/')}/model_card.md"
        
        try:
            print(f"Attempting to download model card from S3: s3://{config.s3_bucket_name}/{s3_card_key}")
            os.makedirs(public_model_dir, exist_ok=True)
            s3_client.download_file(config.s3_bucket_name, s3_card_key, str(public_card_file_path))
            print(f"Successfully downloaded model card from S3 to {public_card_file_path}")
            with open(public_card_file_path, "r", encoding="utf-8") as f:
                return f.read()
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                print(f"Model card not found on S3 at {s3_card_key}")
            else:
                print(f"S3 ClientError when trying to download model card {s3_card_key}: {e}")
        except IOError as e:
            print(f"IOError after downloading model card from S3 {public_card_file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error downloading/reading model card from S3 {s3_card_key}: {e}")
    else:
        print("S3 client not available or bucket not configured. Cannot fetch model card from S3.")
    
    # Fallback: Try to fetch from public models manifest if S3 credentials not available
    if not s3_client or not config.s3_bucket_name:
        print(f"Attempting to fetch model card from public models manifest for {version_str}...")
        public_model_info = _fetch_public_model_info(model_id, revision, config)
        if public_model_info and public_model_info.get('model_card_url'):
            card_url = public_model_info['model_card_url']
            print(f"Found public model card URL: {card_url}")
            try:
                import requests
                response = requests.get(card_url, timeout=10)
                response.raise_for_status()
                
                # Cache the downloaded content locally
                os.makedirs(public_model_dir, exist_ok=True)
                with open(public_card_file_path, "w", encoding="utf-8") as f:
                    f.write(response.text)
                print(f"Successfully downloaded and cached model card from public URL to {public_card_file_path}")
                return response.text
            except Exception as e:
                print(f"Error fetching model card from public URL {card_url}: {e}")
        else:
            print(f"No public model card URL found for {version_str}")
    
    return None

def get_cached_model_config_content(model_id: str, revision: Optional[str] = None, config: Optional[HGLocalizationConfig] = None) -> Optional[Dict[str, Any]]:
    """Retrieves model config content, prioritizing local cache (public then private), then private S3, then public URLs."""
    if config is None:
        config = default_config
        
    version_str = f"(model: {model_id}, revision: {revision or 'default'})"
    
    # Check public cache first (preferred)
    public_model_dir = _get_model_path(model_id, revision, config, is_public=True)
    public_config_file_path = public_model_dir / "config.json"
    
    if public_config_file_path.exists() and public_config_file_path.is_file():
        print(f"Found model config in public cache for {version_str} at {public_config_file_path}")
        try:
            with open(public_config_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error reading public model config {public_config_file_path}: {e}")
    
    # Check private cache second
    private_model_dir = _get_model_path(model_id, revision, config, is_public=False)
    private_config_file_path = private_model_dir / "config.json"
    
    if private_config_file_path.exists() and private_config_file_path.is_file():
        print(f"Found model config in private cache for {version_str} at {private_config_file_path}")
        try:
            with open(private_config_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error reading private model config {private_config_file_path}: {e}")

    print(f"Model config not found locally for {version_str}. Checking S3 (private path)...")
    s3_client = _get_s3_client(config)
    if s3_client and config.s3_bucket_name:
        s3_prefix_path = _get_model_s3_prefix(model_id, revision, config)
        s3_config_key = f"{s3_prefix_path.rstrip('/')}/config.json"
        
        try:
            print(f"Attempting to download model config from S3: s3://{config.s3_bucket_name}/{s3_config_key}")
            os.makedirs(public_model_dir, exist_ok=True)
            s3_client.download_file(config.s3_bucket_name, s3_config_key, str(public_config_file_path))
            print(f"Successfully downloaded model config from S3 to {public_config_file_path}")
            with open(public_config_file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == '404':
                print(f"Model config not found on S3 at {s3_config_key}")
            else:
                print(f"S3 ClientError when trying to download model config {s3_config_key}: {e}")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error after downloading model config from S3 {public_config_file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error downloading/reading model config from S3 {s3_config_key}: {e}")
    else:
        print("S3 client not available or bucket not configured. Cannot fetch model config from S3.")
    
    # Fallback: Try to fetch from public models manifest if S3 credentials not available
    if not s3_client or not config.s3_bucket_name:
        print(f"Attempting to fetch model config from public models manifest for {version_str}...")
        public_model_info = _fetch_public_model_info(model_id, revision, config)
        if public_model_info and public_model_info.get('model_config_url'):
            config_url = public_model_info['model_config_url']
            print(f"Found public model config URL: {config_url}")
            try:
                import requests
                response = requests.get(config_url, timeout=10)
                response.raise_for_status()
                config_data = response.json()
                
                # Cache the downloaded content locally
                os.makedirs(public_model_dir, exist_ok=True)
                with open(public_config_file_path, "w", encoding="utf-8") as f:
                    json.dump(config_data, f, indent=2)
                print(f"Successfully downloaded and cached model config from public URL to {public_config_file_path}")
                return config_data
            except Exception as e:
                print(f"Error fetching model config from public URL {config_url}: {e}")
        else:
            print(f"No public model config URL found for {version_str}")
    
    return None

# --- Public Models Manifest Fetching (via URL) ---

def _fetch_public_models_json_via_url(config: Optional[HGLocalizationConfig] = None) -> Optional[Dict[str, Any]]:
    """Fetches the public_models.json via direct HTTPS GET if config.s3_bucket_name is set."""
    if config is None:
        config = default_config
        
    if not config.s3_bucket_name:
        print("config.s3_bucket_name not configured. Cannot fetch public models JSON.")
        return None

    prefixed_json_key = _get_prefixed_s3_key(config.public_models_json_key, config)
    json_url = _get_s3_public_url(config.s3_bucket_name, prefixed_json_key, config.s3_endpoint_url)
    print(f"Attempting to fetch public models JSON from: {json_url}")
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

def _fetch_public_model_info(model_id: str, revision: Optional[str], config: Optional[HGLocalizationConfig] = None) -> Optional[Dict[str, str]]:
    """Fetches a specific model's public metadata info from public_models.json using direct URL access."""
    if config is None:
        config = default_config
        
    public_config = _fetch_public_models_json_via_url(config)
    if not public_config:
        return None
        
    entry_key = f"{model_id}---{revision or config.default_revision_name}"
    model_info = public_config.get(entry_key)
    if model_info:
        if not all(k in model_info for k in ["model_id", "s3_bucket"]):
            print(f"Error: Public model info for {entry_key} is incomplete. Missing 'model_id' or 's3_bucket'.")
            return None
        print(f"Found public model info for {entry_key}: {model_info}")
        return model_info
    else:
        print(f"Public model info not found for {entry_key} in {config.public_models_json_key}")
        return None

# --- Full Model Download Utilities ---

def _download_full_model_from_hf(model_id: str, revision: Optional[str], local_save_path: Path) -> bool:
    """Downloads the full model (weights, tokenizer, etc.) from Hugging Face Hub.
    
    This function is a placeholder for full model download functionality.
    In a complete implementation, this would use transformers library to download
    all model files including weights, tokenizer, and other necessary files.
    
    Args:
        model_id: The model ID on Hugging Face Hub
        revision: Git revision (branch, tag, commit hash)
        local_save_path: Local path to save the model
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        from transformers import AutoModel, AutoTokenizer, AutoConfig
        
        print(f"Downloading model components for {model_id}...")
        
        # Download config first
        try:
            config = AutoConfig.from_pretrained(model_id, revision=revision)
            config.save_pretrained(str(local_save_path))
            print(f"✓ Downloaded model config")
        except Exception as e:
            print(f"Warning: Failed to download config: {e}")
        
        # Download tokenizer if available
        try:
            tokenizer = AutoTokenizer.from_pretrained(model_id, revision=revision)
            tokenizer.save_pretrained(str(local_save_path))
            print(f"✓ Downloaded tokenizer")
        except Exception as e:
            print(f"Warning: Failed to download tokenizer: {e}")
        
        # Download model weights
        try:
            # Note: This will download the full model weights
            # For large models, this could take a very long time and use significant disk space
            print(f"⚠️  WARNING: Downloading full model weights for {model_id}. This may take a long time and use significant disk space.")
            model = AutoModel.from_pretrained(model_id, revision=revision)
            model.save_pretrained(str(local_save_path))
            print(f"✓ Downloaded model weights")
        except Exception as e:
            print(f"Error: Failed to download model weights: {e}")
            return False
        
        # Also download the model card for completeness
        try:
            card_content = get_model_card_content(model_id, revision=revision)
            if card_content:
                card_file_path = local_save_path / "model_card.md"
                with open(card_file_path, "w", encoding="utf-8") as f:
                    f.write(card_content)
                print(f"✓ Downloaded model card")
        except Exception as e:
            print(f"Warning: Failed to download model card: {e}")
        
        print(f"✓ Full model download completed for {model_id}")
        return True
        
    except ImportError:
        print("Error: transformers library is required for full model download. Install with: pip install transformers")
        return False
    except Exception as e:
        print(f"Error downloading full model: {e}")
        return False

# --- Core Public API Functions ---

def download_model_metadata(model_id: str, revision: Optional[str] = None, make_public: bool = False, skip_s3_upload: bool = False, config: Optional[HGLocalizationConfig] = None, skip_hf_fetch: bool = False, metadata_only: bool = True) -> Tuple[bool, str]:
    """Downloads model metadata (card and config) or full model from Hugging Face, caches locally, and uploads to S3 if configured.
    
    Args:
        model_id: The model ID on Hugging Face Hub
        revision: Git revision (branch, tag, commit hash)
        make_public: Whether to make the model public on S3
        skip_s3_upload: Whether to skip uploading to S3
        config: Configuration object
        skip_hf_fetch: Whether to skip fetching from Hugging Face
        metadata_only: If True, only download metadata (card + config). If False, download full model.
    
    Returns:
        Tuple of (success: bool, path_or_error: str)
    """
    if config is None:
        config = default_config
        
    version_str = f"(revision: {revision or 'default'})"
    download_type = "metadata" if metadata_only else "full model"
    print(f"Processing {download_type}: {model_id} {version_str}")
    
    # Determine if this should be saved to public cache
    save_to_public = make_public
    local_save_path = _get_model_path(model_id, revision, config, is_public=save_to_public)

    # Check if the target model already exists in the intended location
    def _check_model_exists(path: Path) -> bool:
        if not path.exists():
            return False
        
        if metadata_only:
            # For metadata-only, check for card or config
            return ((path / "model_card.md").exists() or (path / "config.json").exists())
        else:
            # For full model, check for more comprehensive files
            # This is a basic check - could be enhanced based on model type
            has_config = (path / "config.json").exists()
            has_weights = any(path.glob("*.bin")) or any(path.glob("*.safetensors")) or (path / "pytorch_model.bin").exists()
            return has_config and (has_weights or (path / "model.safetensors").exists())

    if save_to_public:
        # For public cache, only check the public path
        if _check_model_exists(local_save_path):
            print(f"Model {download_type} {model_id} {version_str} already exists in public cache at {local_save_path}")
            return True, str(local_save_path)
    else:
        # For private cache, check public path first (preferred), then private path
        public_path = _get_model_path(model_id, revision, config, is_public=True)
        private_path = _get_model_path(model_id, revision, config, is_public=False)
        
        # Check public path first
        if _check_model_exists(public_path):
            print(f"Model {download_type} {model_id} {version_str} already exists in public cache at {public_path}")
            return True, str(public_path)
        
        # Then check private path
        if _check_model_exists(private_path):
            print(f"Model {download_type} {model_id} {version_str} already exists in private cache at {private_path}")
            return True, str(private_path)

    # Check S3 for existing model data
    s3_client = _get_s3_client(config)
    if s3_client and config.s3_bucket_name:
        s3_prefix_path_for_model = _get_model_s3_prefix(model_id, revision, config)
        print(f"Checking S3 for {download_type} {model_id} {version_str} at s3://{config.s3_bucket_name}/{s3_prefix_path_for_model}...")
        
        # Check what exists on S3 based on download type
        if metadata_only:
            # For metadata-only, check for card or config
            card_exists = False
            config_exists = False
            try:
                s3_client.head_object(Bucket=config.s3_bucket_name, Key=f"{s3_prefix_path_for_model}/model_card.md")
                card_exists = True
            except ClientError:
                pass
            
            try:
                s3_client.head_object(Bucket=config.s3_bucket_name, Key=f"{s3_prefix_path_for_model}/config.json")
                config_exists = True
            except ClientError:
                pass
            
            s3_has_data = card_exists or config_exists
        else:
            # For full model, check for config and at least one weight file
            # This is a simplified check - in practice, you'd want more sophisticated detection
            config_exists = False
            weights_exist = False
            
            try:
                s3_client.head_object(Bucket=config.s3_bucket_name, Key=f"{s3_prefix_path_for_model}/config.json")
                config_exists = True
            except ClientError:
                pass
            
            # Check for common weight file patterns
            weight_patterns = ["pytorch_model.bin", "model.safetensors"]
            for pattern in weight_patterns:
                try:
                    s3_client.head_object(Bucket=config.s3_bucket_name, Key=f"{s3_prefix_path_for_model}/{pattern}")
                    weights_exist = True
                    break
                except ClientError:
                    continue
            
            s3_has_data = config_exists and weights_exist
        
        if s3_has_data:
            print(f"Model {download_type} found on S3. Attempting to download from S3 to local cache: {local_save_path}...")
            if _download_directory_from_s3(s3_client, local_save_path, config.s3_bucket_name, s3_prefix_path_for_model):
                print(f"Successfully downloaded {download_type} from S3 to {local_save_path}.")
                if _check_model_exists(local_save_path):
                    # Store bucket metadata for the downloaded model
                    _store_model_bucket_metadata(model_id, revision, config, is_public=save_to_public)
                    return True, str(local_save_path)
                else:
                    print(f"Error: S3 download reported success, but {download_type} not found or incomplete at {local_save_path}. Proceeding to Hugging Face.")
            else:
                print(f"Failed to download {download_type} from S3. Will attempt Hugging Face download.")
        else:
            print(f"Model {download_type} not found on S3. Will attempt Hugging Face download.")
    else:
        print("S3 not configured or client init failed; skipping S3 check.")

    # Download from Hugging Face
    revision_str_part = f" (revision: {revision})" if revision else ""
    print(f"Downloading {download_type} '{model_id}' from Hugging Face{revision_str_part}...")
    
    try:
        os.makedirs(local_save_path, exist_ok=True)
        
        if not skip_hf_fetch:
            if metadata_only:
                # Download only metadata (card + config)
                print(f"Downloading metadata only for {model_id}...")
                
                # Download model card
                card_content_md = get_model_card_content(model_id, revision=revision)
                if card_content_md:
                    card_file_path = local_save_path / "model_card.md"
                    try:
                        with open(card_file_path, "w", encoding="utf-8") as f:
                            f.write(card_content_md)
                        print(f"Successfully saved model card to {card_file_path}")
                    except IOError as e:
                        print(f"Warning: Failed to save model card to {card_file_path}: {e}")
                else:
                    print(f"Warning: Could not retrieve model card for {model_id} {version_str}.")
                
                # Download model config
                config_content = get_model_config_content(model_id, revision=revision)
                if config_content:
                    config_file_path = local_save_path / "config.json"
                    try:
                        with open(config_file_path, "w", encoding="utf-8") as f:
                            json.dump(config_content, f, indent=2)
                        print(f"Successfully saved model config to {config_file_path}")
                    except IOError as e:
                        print(f"Warning: Failed to save model config to {config_file_path}: {e}")
                else:
                    print(f"Warning: Could not retrieve model config for {model_id} {version_str}.")
            else:
                # Download full model
                print(f"Downloading full model for {model_id}...")
                success = _download_full_model_from_hf(model_id, revision, local_save_path)
                if not success:
                    raise Exception("Failed to download full model from Hugging Face")
        else:
            print(f"Skipping Hugging Face {download_type} fetch for {model_id} {version_str} (isolated environment mode).")

        print(f"Model {download_type} '{model_id}' {version_str} successfully saved to local cache: {local_save_path}")
        
        # Store bucket metadata for the downloaded model
        _store_model_bucket_metadata(model_id, revision, config, is_public=save_to_public)

        # Upload to S3 if configured and not skipped
        if not skip_s3_upload:
            s3_client_for_upload = _get_s3_client(config)
            if s3_client_for_upload and config.s3_bucket_name:
                s3_prefix_path_for_upload = _get_model_s3_prefix(model_id, revision, config)
                _upload_directory_to_s3(s3_client_for_upload, local_save_path, config.s3_bucket_name, s3_prefix_path_for_upload)
                
                # Update private index for non-public uploads
                if not make_public:
                    print(f"Updating private models index for {model_id} {version_str}...")
                    _update_private_models_index(s3_client_for_upload, config.s3_bucket_name, model_id, revision, config)
                
                # Implement public model metadata functionality similar to datasets
                if make_public:
                    print(f"Making model metadata public for {model_id} {version_str}...")
                    
                    # Make model metadata files (card and config) public
                    if _make_model_metadata_public(s3_client_for_upload, config.s3_bucket_name, model_id, revision, local_save_path, config):
                        print(f"Successfully made model metadata files public")
                        
                        # Update the public models manifest
                        if _update_public_models_json(s3_client_for_upload, config.s3_bucket_name, model_id, revision, config):
                            print(f"Successfully updated public models manifest for {model_id} {version_str}")
                        else:
                            print(f"Warning: Failed to update public models manifest for {model_id} {version_str}")
                    else:
                        print(f"Warning: Failed to make some model metadata files public for {model_id} {version_str}")
        
        return True, str(local_save_path)

    except Exception as e:
        print(f"An error occurred while processing '{model_id}' {version_str}: {e}")
        if local_save_path.exists():
            try:
                shutil.rmtree(local_save_path)
                print(f"Cleaned up partially saved data at {local_save_path}")
            except Exception as cleanup_e:
                print(f"Error during cleanup of {local_save_path}: {cleanup_e}")
        return False, str(e)

def list_local_models(config: Optional[HGLocalizationConfig] = None, public_access_only: bool = False, filter_by_bucket: bool = True) -> List[Dict[str, str]]:
    """Lists models available in the local cache."""
    if config is None:
        config = default_config
        
    available_models = []
    
    # Determine which directories to scan based on access mode
    if public_access_only:
        # Public access only - scan public directory only
        if config.public_models_store_path.exists():
            directories_to_scan = [(config.public_models_store_path, True)]
            print(f"Public access mode: scanning public models only")
        else:
            print(f"Local model store directory does not exist: {config.public_models_store_path}")
            return available_models
    else:
        # Private access - scan both public and private directories
        directories_to_scan = []
        if config.public_models_store_path.exists():
            directories_to_scan.append((config.public_models_store_path, True))
        if config.models_store_path.exists():
            directories_to_scan.append((config.models_store_path, False))
        print(f"Private access mode: scanning both public and private models")
        
        if not directories_to_scan:
            print(f"Local model store directory does not exist: {config.models_store_path}")
            return available_models
    
    for store_path, is_public_store in directories_to_scan:
        models_from_store = _scan_model_directory(store_path, config, is_public_store, filter_by_bucket)
        
        # Merge models, handling duplicates (prefer public over private)
        for model_info in models_from_store:
            # Check for duplicates (same model in both public and private)
            # Prefer public version if it exists
            existing_idx = None
            for i, existing in enumerate(available_models):
                if (existing["model_id"] == model_info["model_id"] and 
                    existing["revision"] == model_info["revision"]):
                    existing_idx = i
                    break
            
            if existing_idx is not None:
                # Model already exists, prefer public version
                if is_public_store:
                    available_models[existing_idx] = model_info
            else:
                available_models.append(model_info)
    
    if not available_models:
        print("No local models found in cache.")
    else:
        print(f"Found {len(available_models)} local model(s):")
        for model_info in available_models:
            model_type = "Full Model" if model_info.get('is_full_model', False) else "Metadata Only"
            print(f"  Model ID: {model_info['model_id']}, "
                  f"Revision: {model_info.get('revision', config.default_revision_name)}, "
                  f"Type: {model_type}, "
                  f"Path: {model_info['path']}, "
                  f"Card: {'Yes' if model_info['has_card'] else 'No'}, "
                  f"Config: {'Yes' if model_info['has_config'] else 'No'}")
    return available_models

def list_s3_models(config: Optional[HGLocalizationConfig] = None) -> List[Dict[str, str]]:
    """Lists models available on S3."""
    if config is None:
        config = default_config
        
    available_s3_models = []
    if not config.s3_bucket_name:
        print("config.s3_bucket_name not configured. Cannot list S3 models.")
        return []

    # Try to use private index first for much faster performance
    if config.aws_access_key_id and config.aws_secret_access_key:
        print("Attempting to list S3 models from private index (fast method)...")
        private_index = _fetch_private_models_index(config)
        if private_index:
            print(f"Successfully fetched private models index with {len(private_index)} entries.")
            for entry_key, entry_data in private_index.items():
                if isinstance(entry_data, dict) and all(k in entry_data for k in ["model_id", "s3_prefix"]):
                    # Generate S3 card URL if card exists
                    s3_card_url = None
                    if entry_data.get("has_card", False):
                        card_key = f"{entry_data['s3_prefix']}/model_card.md"
                        s3_card_url = _get_s3_public_url(config.s3_bucket_name, card_key, config.s3_endpoint_url)
                    
                    # Generate S3 config URL if config exists
                    s3_config_url = None
                    if entry_data.get("has_config", False):
                        config_key = f"{entry_data['s3_prefix']}/config.json"
                        s3_config_url = _get_s3_public_url(config.s3_bucket_name, config_key, config.s3_endpoint_url)
                    
                    available_s3_models.append({
                        "model_id": entry_data["model_id"],
                        "revision": entry_data.get("revision"),
                        "has_card": entry_data.get("has_card", False),
                        "has_config": entry_data.get("has_config", False),
                        "has_tokenizer": entry_data.get("has_tokenizer", False),
                        "is_full_model": entry_data.get("is_full_model", False),
                        "s3_card_url": s3_card_url,
                        "s3_config_url": s3_config_url
                    })
                else:
                    print(f"Skipping malformed entry in private models index: {entry_key}")
            
            if available_s3_models:
                print(f"Listed {len(available_s3_models)} S3 models from private index.")
                return available_s3_models
        else:
            print("Private models index not found or could not be fetched. Falling back to bucket scanning...")
            
            # Fallback to slow bucket scanning method
            s3_client = _get_s3_client(config)
            if s3_client:
                print("Listing S3 models via authenticated API call (scanning bucket structure - slow method)...")
                paginator = s3_client.get_paginator('list_objects_v2')
                
                # scan_base_prefix should be the S3_DATA_PREFIX + "models/", ensuring it ends with a slash if not empty
                scan_base_prefix = config.s3_data_prefix.strip('/') + '/' if config.s3_data_prefix else ""
                scan_base_prefix += "models/"
                
                try:
                    # Level 1: List model_id directories
                    level1_pages = paginator.paginate(
                        Bucket=config.s3_bucket_name,
                        Prefix=scan_base_prefix,
                        Delimiter='/'
                    )
                    
                    for page in level1_pages:
                        common_prefixes = page.get('CommonPrefixes', [])
                        for prefix_info in common_prefixes:
                            model_id_prefix = prefix_info['Prefix']
                            # Extract model_id from prefix
                            model_id_safe = model_id_prefix[len(scan_base_prefix):].rstrip('/')
                            if not model_id_safe:
                                continue
                            
                            # Restore original model_id
                            model_id_orig = _restore_dataset_name(model_id_safe)
                            
                            # Level 2: List revision directories under this model_id
                            level2_pages = paginator.paginate(
                                Bucket=config.s3_bucket_name,
                                Prefix=model_id_prefix,
                                Delimiter='/'
                            )
                            
                            for level2_page in level2_pages:
                                level2_common_prefixes = level2_page.get('CommonPrefixes', [])
                                for level2_prefix_info in level2_common_prefixes:
                                    revision_prefix = level2_prefix_info['Prefix']
                                    # Extract revision from prefix
                                    revision_safe = revision_prefix[len(model_id_prefix):].rstrip('/')
                                    if not revision_safe:
                                        continue
                                    
                                    # For revisions, don't use _restore_dataset_name since revisions don't contain slashes
                                    # Just use the revision_safe directly
                                    revision_display = revision_safe if revision_safe != config.default_revision_name else None
                                    
                                    # Check if this model has a card
                                    s3_card_key = f"{revision_prefix}model_card.md"
                                    has_card = False
                                    try:
                                        s3_client.head_object(Bucket=config.s3_bucket_name, Key=s3_card_key)
                                        has_card = True
                                    except ClientError:
                                        pass
                                    
                                    # Check if this model has a config
                                    s3_config_key = f"{revision_prefix}config.json"
                                    has_config = False
                                    try:
                                        s3_client.head_object(Bucket=config.s3_bucket_name, Key=s3_config_key)
                                        has_config = True
                                    except ClientError:
                                        pass
                                    
                                    # Check if this is a full model (has weights)
                                    is_full_model = False
                                    weight_patterns = ["pytorch_model.bin", "model.safetensors"]
                                    for pattern in weight_patterns:
                                        try:
                                            s3_client.head_object(Bucket=config.s3_bucket_name, Key=f"{revision_prefix}{pattern}")
                                            is_full_model = True
                                            break
                                        except ClientError:
                                            continue
                                    
                                    # Check for tokenizer files
                                    has_tokenizer = False
                                    tokenizer_patterns = ["tokenizer.json", "tokenizer_config.json"]
                                    for pattern in tokenizer_patterns:
                                        try:
                                            s3_client.head_object(Bucket=config.s3_bucket_name, Key=f"{revision_prefix}{pattern}")
                                            has_tokenizer = True
                                            break
                                        except ClientError:
                                            continue
                                    
                                    # Generate S3 card URL if card exists
                                    s3_card_url = None
                                    if has_card:
                                        s3_card_url = _get_s3_public_url(config.s3_bucket_name, s3_card_key, config.s3_endpoint_url)
                                    
                                    model_info = {
                                        "model_id": model_id_orig,
                                        "revision": revision_display,
                                        "has_card": has_card,
                                        "has_config": has_config,
                                        "has_tokenizer": has_tokenizer,
                                        "is_full_model": is_full_model,
                                        "s3_card_url": s3_card_url
                                    }
                                    available_s3_models.append(model_info)
                    
                except ClientError as e:
                    print(f"Error listing S3 models: {e}")
                    return []
                except Exception as e:
                    print(f"Unexpected error listing S3 models: {e}")
                    return []
            else:
                print("AWS credentials not configured. Cannot list S3 models via authenticated API.")

    # Fallback or primary method if no AWS creds: list from public_models.json
    # Only do this if we haven't populated available_s3_models from scanning yet.
    if not available_s3_models:
        print("Attempting to list S3 models from public_models.json...")
        public_json_content = _fetch_public_models_json_via_url(config)
        if public_json_content:
            public_models_from_json = []
            for entry_key, entry_data in public_json_content.items():
                if isinstance(entry_data, dict) and all(k in entry_data for k in ["model_id", "s3_bucket"]):
                    # Extract model card and config URLs if available
                    model_card_url = entry_data.get("model_card_url")
                    model_config_url = entry_data.get("model_config_url")
                    
                    public_models_from_json.append({
                        "model_id": entry_data.get("model_id"),  # This should already be in original format from public JSON
                        "revision": entry_data.get("revision"),  # Already None if not present or was default
                        "has_card": bool(model_card_url),
                        "has_config": bool(model_config_url),
                        "has_tokenizer": False,  # Not tracked in public manifest
                        "is_full_model": False,  # Public models are metadata-only
                        "s3_card_url": model_card_url,
                        "s3_config_url": model_config_url
                    })
                else:
                    print(f"Skipping malformed entry in public_models.json: {entry_key}")
            
            if public_models_from_json:
                print("Listing S3 models based on public_models.json.")
                available_s3_models.extend(public_models_from_json)  # Extend, in case authenticated scan found some but errored before completing
            else:
                print(f"No models found or listed in {config.public_models_json_key} at public URL (or it was empty).")
        else:
            print(f"Could not fetch or parse {config.public_models_json_key} from public URL.")

    if not available_s3_models:
        print(f"No models found in S3 bucket '{config.s3_bucket_name}' by any method.")
    else:
        print(f"Found {len(available_s3_models)} S3 model(s):")
        for model_info in available_s3_models:
            model_type = "Full Model" if model_info.get('is_full_model', False) else "Metadata Only"
            print(f"  Model ID: {model_info['model_id']}, "
                  f"Revision: {model_info.get('revision', config.default_revision_name)}, "
                  f"Type: {model_type}, "
                  f"Card: {'Yes' if model_info['has_card'] else 'No'}, "
                  f"Config: {'Yes' if model_info['has_config'] else 'No'}")
    
    return available_s3_models

def _scan_model_directory(store_path: Path, config: HGLocalizationConfig, is_public_store: bool, filter_by_bucket: bool, include_legacy: bool = True) -> List[Dict[str, str]]:
    """Scan a model directory for both new bucket-specific and legacy storage structures."""
    models = []
    
    if not store_path.exists():
        return models
    
    # Scan new bucket-specific structure: store_path/by_bucket/bucket_id/model_id/revision
    if config.s3_bucket_name:
        by_bucket_path = store_path / "by_bucket"
        if by_bucket_path.exists():
            for bucket_dir in by_bucket_path.iterdir():
                if bucket_dir.is_dir() and not bucket_dir.name.startswith("."):
                    models.extend(_scan_legacy_model_structure(bucket_dir, config, is_public_store, filter_by_bucket))
    
    # Scan legacy structure: store_path/model_id/revision (for backward compatibility)
    if include_legacy:
        models.extend(_scan_legacy_model_structure(store_path, config, is_public_store, filter_by_bucket))
    
    return models

def _scan_legacy_model_structure(base_path: Path, config: HGLocalizationConfig, is_public_store: bool, filter_by_bucket: bool) -> List[Dict[str, str]]:
    """Scan the legacy model storage structure."""
    models = []
    
    for model_id_dir in base_path.iterdir():
        if model_id_dir.is_dir() and not model_id_dir.name.startswith(".") and model_id_dir.name != "by_bucket":
            for revision_dir in model_id_dir.iterdir():
                if revision_dir.is_dir():
                    # Check if this directory contains model metadata
                    has_card = (revision_dir / "model_card.md").is_file()
                    has_config = (revision_dir / "config.json").is_file()
                    
                    if has_card or has_config:
                        # Convert safe names back to original format
                        model_id_display = _restore_dataset_name(model_id_dir.name)  # Reuse the same function
                        revision_display = revision_dir.name
                        
                        # Check if this is a full model or metadata-only
                        has_weights = any(revision_dir.glob("*.bin")) or any(revision_dir.glob("*.safetensors")) or (revision_dir / "pytorch_model.bin").exists()
                        has_tokenizer = (revision_dir / "tokenizer.json").exists() or (revision_dir / "tokenizer_config.json").exists()
                        is_full_model = has_config and (has_weights or (revision_dir / "model.safetensors").exists())
                        
                        # TODO: Implement bucket filtering for models if needed
                        # For now, include all models
                        
                        model_info = {
                            "model_id": model_id_display,
                            "revision": revision_display if revision_display != config.default_revision_name else None,
                            "path": str(revision_dir),
                            "has_card": has_card,
                            "has_config": has_config,
                            "has_tokenizer": has_tokenizer,
                            "is_full_model": is_full_model,
                            "is_public": is_public_store
                        }
                        models.append(model_info)
    
    return models

def _check_s3_model_exists(s3_client: Any, bucket_name: str, s3_prefix_for_model_version: str) -> bool:
    """Checks if a model (marker files) exists at the given S3 prefix for the model version."""
    if not s3_client or not bucket_name:
        return False
    try:
        # Check for model card first
        s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix_for_model_version.rstrip('/')}/model_card.md")
        return True
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == '404':
            try:
                # Check for config.json as fallback
                s3_client.head_object(Bucket=bucket_name, Key=f"{s3_prefix_for_model_version.rstrip('/')}/config.json")
                return True
            except ClientError as e2:
                if e2.response.get('Error', {}).get('Code') == '404':
                    return False
                return False
        return False
    except Exception:
        return False

def sync_local_model_to_s3(model_id: str, revision: Optional[str] = None, make_public: bool = False, config: Optional[HGLocalizationConfig] = None) -> Tuple[bool, str]:
    """Syncs a specific local model to S3. Uploads if not present; can also make public."""
    if config is None:
        config = default_config
        
    version_str = f"(revision: {revision or 'default'})"
    print(f"Attempting to sync local model to S3: {model_id} {version_str}")

    # Check both public and private paths for the model
    public_path = _get_model_path(model_id, revision, config, is_public=True)
    private_path = _get_model_path(model_id, revision, config, is_public=False)
    
    # Determine which local path to use (prefer public if it exists)
    local_save_path = None
    if public_path.exists() and ((public_path / "model_card.md").exists() or (public_path / "config.json").exists()):
        local_save_path = public_path
        print(f"Found model in public cache at {public_path}")
    elif private_path.exists() and ((private_path / "model_card.md").exists() or (private_path / "config.json").exists()):
        local_save_path = private_path
        print(f"Found model in private cache at {private_path}")

    if not local_save_path:
        msg = f"Local model {model_id} {version_str} not found or is incomplete. Cannot sync."
        print(msg)
        return False, msg

    s3_client = _get_s3_client(config)
    if not s3_client or not config.s3_bucket_name:
        msg = "S3 not configured (bucket name or client init failed). Cannot sync to S3."
        if make_public: msg += " Cannot make model public."
        print(msg)
        return False, msg

    # s3_prefix_path_for_model includes the S3_DATA_PREFIX
    s3_prefix_path_for_model = _get_model_s3_prefix(model_id, revision, config)
    private_s3_copy_exists = _check_s3_model_exists(s3_client, config.s3_bucket_name, s3_prefix_path_for_model)

    if private_s3_copy_exists:
        print(f"Model {model_id} {version_str} already exists as private S3 copy at s3://{config.s3_bucket_name}/{s3_prefix_path_for_model}.")
    else:
        print(f"Model {model_id} {version_str} not found on S3 (private). Uploading from {local_save_path} to s3://{config.s3_bucket_name}/{s3_prefix_path_for_model}")
        try:
            _upload_directory_to_s3(s3_client, local_save_path, config.s3_bucket_name, s3_prefix_path_for_model)
            print(f"Successfully uploaded model '{model_id}' {version_str} to S3 (private).")
            private_s3_copy_exists = True
            
            # Update private index for non-public uploads
            if not make_public:
                print(f"Updating private models index for {model_id} {version_str}...")
                _update_private_models_index(s3_client, config.s3_bucket_name, model_id, revision, config)
        except Exception as e:
            msg = f"Error uploading model '{model_id}' {version_str} to S3 (private): {e}"
            print(msg)
            return False, msg

    if make_public and private_s3_copy_exists:
        print(f"Processing --make-public for model {model_id} {version_str}...")
        
        # Make model metadata files (card and config) public
        if _make_model_metadata_public(s3_client, config.s3_bucket_name, model_id, revision, local_save_path, config):
            print(f"Successfully made model metadata files public")
            
            # Update the public models manifest
            if _update_public_models_json(s3_client, config.s3_bucket_name, model_id, revision, config):
                print(f"Successfully updated public models manifest for {model_id} {version_str}")
            else:
                print(f"Warning: Failed to update public models manifest for {model_id} {version_str}")
        else:
            print(f"Warning: Failed to make some model metadata files public for {model_id} {version_str}")
    elif make_public and not private_s3_copy_exists:
        print(f"Cannot make {model_id} {version_str} public because its private S3 copy does not exist or failed to upload.")

    final_msg = f"Sync process for {model_id} {version_str} completed."
    print(final_msg)
    return True, final_msg

def sync_all_local_models_to_s3(make_public: bool = False, config: Optional[HGLocalizationConfig] = None) -> None:
    """Iterates through all local models and attempts to sync them to S3."""
    if config is None:
        config = default_config
        
    print(f"Starting sync of all local models to S3. Make public: {make_public}")
    # Use filter_by_bucket=False to sync all local models regardless of bucket configuration
    local_models = list_local_models(config, filter_by_bucket=False)
    if not local_models:
        print("No local models found in cache to sync.")
        return

    succeeded_syncs = 0
    failed_syncs = 0
    
    s3_client_check = _get_s3_client(config) # Check once if S3 is usable
    if not s3_client_check or not config.s3_bucket_name:
        print("S3 not configured (bucket name or client init failed). Cannot sync any models to S3.")
        if make_public: print("Cannot make models public.")
        return

    for model_info in local_models:
        model_id = model_info['model_id']
        revision = model_info.get('revision')     
        
        print(f"\n--- Processing local model for sync: ID='{model_id}', Revision='{revision}' ---")
        # sync_local_model_to_s3 will use its own S3 client instance
        success, message = sync_local_model_to_s3(model_id, revision, make_public=make_public, config=config)
        if success:
            succeeded_syncs += 1
        else:
            failed_syncs += 1
            
    print("\n--- Sync all local models to S3 finished ---")
    print(f"Total local models processed: {len(local_models)}")
    print(f"Successfully processed (primary sync action): {succeeded_syncs}")
    print(f"Failed to process (see logs for errors): {failed_syncs}") 