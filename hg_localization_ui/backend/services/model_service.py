from typing import List, Optional, Dict, Any

from models import ModelInfo, CodeExample, ModelCard, ModelConfig
from config import is_public_access_only
from hg_localization.model_manager import (
    download_model_metadata,
    list_local_models,
    list_s3_models,
    get_cached_model_card_content,
    get_cached_model_config_content,
    get_model_card_content,
    get_model_config_content
)

def get_cached_models_service(config, public_only: bool, filter_by_bucket: bool = True) -> List[ModelInfo]:
    """Get list of cached models"""
    models = list_local_models(config=config, public_access_only=public_only, filter_by_bucket=filter_by_bucket)
    return [
        ModelInfo(
            model_id=model["model_id"],
            revision=model.get("revision"),
            path=model.get("path"),
            has_card=model.get("has_card", False),
            has_config=model.get("has_config", False),
            has_tokenizer=model.get("has_tokenizer", False),
            is_full_model=model.get("is_full_model", False),
            source="cached",
            is_cached=True,
            available_s3=False
        )
        for model in models
    ]

def get_s3_models_service(config) -> List[ModelInfo]:
    """Get list of S3 models"""
    models = list_s3_models(config=config)
    return [
        ModelInfo(
            model_id=model["model_id"],
            revision=model.get("revision"),
            has_card=model.get("has_card", False),
            has_config=model.get("has_config", False),
            has_tokenizer=model.get("has_tokenizer", False),
            is_full_model=model.get("is_full_model", False),
            source="s3",
            is_cached=False,
            available_s3=True
        )
        for model in models
    ]

def get_all_models_service(config) -> List[ModelInfo]:
    """Get combined list of cached and S3 models"""
    # Get cached models that match the current bucket configuration
    try:
        public_only = is_public_access_only(config)
        cached_models = get_cached_models_service(config, public_only, filter_by_bucket=True)
    except Exception:
        cached_models = []
    
    # Get S3 models
    s3_models = []
    if config and config.s3_bucket_name:
        try:
            s3_models = get_s3_models_service(config)
        except Exception:
            pass  # S3 might not be accessible
    
    # Combine and deduplicate
    all_models = {}
    
    # Add cached models
    for model in cached_models:
        key = f"{model.model_id}_{model.revision}"
        all_models[key] = model
    
    # Add S3 models (mark as available in S3)
    for model in s3_models:
        key = f"{model.model_id}_{model.revision}"
        if key in all_models:
            # Model exists both cached and in S3
            existing = all_models[key]
            existing.available_s3 = True
            existing.source = "both"
            if not existing.has_card and model.has_card:
                existing.has_card = model.has_card
            if not existing.has_config and model.has_config:
                existing.has_config = model.has_config
            if not existing.has_tokenizer and model.has_tokenizer:
                existing.has_tokenizer = model.has_tokenizer
            if not existing.is_full_model and model.is_full_model:
                existing.is_full_model = model.is_full_model
        else:
            # Model only in S3
            all_models[key] = model
    
    return list(all_models.values())

def get_model_card_service(model_id: str, revision: Optional[str], try_huggingface: bool, config) -> str:
    """Get model card content"""
    # First try to get cached card content (local cache and S3)
    card_content = get_cached_model_card_content(model_id, revision, config=config)
    
    # Only try Hugging Face if explicitly requested and no cached content found
    if not card_content and try_huggingface:
        print(f"Attempting to fetch model card from Hugging Face for {model_id}")
        card_content = get_model_card_content(model_id, revision)
    
    if not card_content:
        raise ValueError("Model card not found")
    
    return card_content

def get_model_config_service(model_id: str, revision: Optional[str], try_huggingface: bool, config) -> Dict[str, Any]:
    """Get model config content"""
    # First try to get cached config content (local cache and S3)
    config_content = get_cached_model_config_content(model_id, revision, config=config)
    
    # Only try Hugging Face if explicitly requested and no cached content found
    if not config_content and try_huggingface:
        print(f"Attempting to fetch model config from Hugging Face for {model_id}")
        config_content = get_model_config_content(model_id, revision)
    
    if not config_content:
        raise ValueError("Model config not found")
    
    return config_content

def get_model_examples_service(model_id: str, revision: Optional[str]) -> List[CodeExample]:
    """Get code examples for using the model"""
    examples = []
    
    # Convert model_id back to original format for Hugging Face operations
    original_model_id = model_id.replace('_', '/')
    
    # Basic loading example
    revision_part = f", revision='{revision}'" if revision else ""
    
    basic_code = f"""from hg_localization import get_cached_model_card_content, get_cached_model_config_content

# Load the model metadata from local cache
card_content = get_cached_model_card_content(
    model_id='{original_model_id}'{revision_part}
)

config_content = get_cached_model_config_content(
    model_id='{original_model_id}'{revision_part}
)

if card_content:
    print("Model card loaded successfully!")
    print(card_content[:200] + "...")

if config_content:
    print(f"Model type: {{config_content.get('model_type')}}")
    print(f"Architecture: {{config_content.get('architectures', [])}}")"""
    
    examples.append(CodeExample(
        title="Load Model Metadata",
        description="Load the model card and config from local cache using hg_localization",
        code=basic_code
    ))
    
    # Download example
    download_code = f"""from hg_localization import download_model_metadata

# Download model metadata from Hugging Face and cache locally
success, path = download_model_metadata(
    model_id='{original_model_id}'{revision_part},
    metadata_only=True  # Only download metadata (fast)
)

if success:
    print(f"Model metadata downloaded to: {{path}}")
else:
    print(f"Download failed: {{path}}")"""
    
    examples.append(CodeExample(
        title="Download Model Metadata",
        description="Download the model metadata from Hugging Face Hub",
        code=download_code
    ))
    
    # Full model download example
    full_download_code = f"""from hg_localization import download_model_metadata

# Download full model (including weights and tokenizer)
success, path = download_model_metadata(
    model_id='{original_model_id}'{revision_part},
    metadata_only=False  # Download full model (large!)
)

if success:
    print(f"Full model downloaded to: {{path}}")
else:
    print(f"Download failed: {{path}}")"""
    
    examples.append(CodeExample(
        title="Download Full Model",
        description="Download the complete model including weights and tokenizer",
        code=full_download_code
    ))
    
    # Direct Hugging Face usage
    hf_code = f"""from transformers import AutoModel, AutoTokenizer, AutoConfig

# Load directly from Hugging Face Hub
model = AutoModel.from_pretrained('{original_model_id}'{revision_part})
tokenizer = AutoTokenizer.from_pretrained('{original_model_id}'{revision_part})
config = AutoConfig.from_pretrained('{original_model_id}'{revision_part})

print(f"Model loaded: {{type(model).__name__}}")
print(f"Tokenizer vocab size: {{tokenizer.vocab_size}}")"""
    
    examples.append(CodeExample(
        title="Direct Hugging Face Usage",
        description="Load the model directly from Hugging Face Hub using transformers",
        code=hf_code
    ))
    
    return examples

async def cache_model_task(model_id: str, revision: Optional[str], make_public: bool, 
                          metadata_only: bool, config, manager):
    """Background task for caching a model"""
    try:
        download_type = "metadata" if metadata_only else "full model"
        await manager.broadcast(f"Starting caching of {download_type} for {model_id}...")
        
        success, message = download_model_metadata(
            model_id=model_id,
            revision=revision,
            make_public=make_public,
            config=config,
            skip_hf_fetch=True,  # Skip HF fetch in isolated server environment
            metadata_only=metadata_only
        )
        
        if success:
            await manager.broadcast(f"Successfully cached {download_type} for {model_id}")
        else:
            await manager.broadcast(f"Failed to cache {download_type} for {model_id}: {message}")
            
    except Exception as e:
        await manager.broadcast(f"Error caching {model_id}: {str(e)}") 