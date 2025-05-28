# Model Localization with HG-Localization

This document describes the model localization functionality added to HG-Localization, which allows you to cache model metadata (model cards and config.json files) locally and optionally sync them to S3.

## Overview

The model localization feature provides a lightweight way to cache essential model information without downloading the full model weights. This is particularly useful for:

- **Model Discovery**: Browse and explore model metadata without downloading large files
- **Offline Access**: Access model cards and configurations when offline
- **Team Collaboration**: Share model metadata across team members via S3
- **Documentation**: Keep local copies of model documentation for reference

## What Gets Downloaded

The model localization feature supports two modes:

### Metadata Only (Default)
1. **Model Card** (`model_card.md`): The README/documentation from the model repository
2. **Model Config** (`config.json`): The model configuration file containing architecture details

### Full Model (Optional)
1. **Model Card** (`model_card.md`): The README/documentation from the model repository
2. **Model Config** (`config.json`): The model configuration file containing architecture details
3. **Model Weights** (`*.bin`, `*.safetensors`): The actual model parameters
4. **Tokenizer** (`tokenizer.json`, `tokenizer_config.json`, etc.): Text processing components
5. **Additional Files**: Any other files needed for the model to function

## CLI Commands

### Download Model

```bash
# Download model metadata only (default - fast and lightweight)
python -m hg_localization.cli download-model bert-base-uncased

# Download full model (includes weights, tokenizer, etc. - large download!)
python -m hg_localization.cli download-model bert-base-uncased --full-model

# Download with specific revision
python -m hg_localization.cli download-model bert-base-uncased --revision v1.0

# Download without uploading to S3 (local only)
python -m hg_localization.cli download-model bert-base-uncased --no-s3-upload

# Download and make public (if S3 is configured)
python -m hg_localization.cli download-model bert-base-uncased --make-public
```

### List Local Models

```bash
# List all cached model metadata
python -m hg_localization.cli list-local-models
```

### View Model Information

```bash
# Show the model card
python -m hg_localization.cli show-model-card bert-base-uncased

# Show the model card and try fetching from HF if not cached
python -m hg_localization.cli show-model-card bert-base-uncased --try-huggingface

# Show the model configuration
python -m hg_localization.cli show-model-config bert-base-uncased
```

## Programmatic API

### Download Model

```python
import hg_localization

# Download model metadata only (default)
success, path = hg_localization.download_model_metadata(
    model_id="bert-base-uncased",
    revision=None,  # Use default revision
    make_public=False,  # Don't make public
    skip_s3_upload=False,  # Upload to S3 if configured
    skip_hf_fetch=False,  # Fetch from Hugging Face
    metadata_only=True  # Only download metadata (default)
)

# Download full model (including weights)
success, path = hg_localization.download_model_metadata(
    model_id="bert-base-uncased",
    metadata_only=False  # Download full model
)

if success:
    print(f"Model saved to: {path}")
else:
    print(f"Error: {path}")
```

### List Local Models

```python
import hg_localization

# List all cached models
models = hg_localization.list_local_models()

for model in models:
    print(f"Model: {model['model_id']}")
    print(f"  Revision: {model.get('revision', 'default')}")
    print(f"  Has Card: {model['has_card']}")
    print(f"  Has Config: {model['has_config']}")
    print(f"  Path: {model['path']}")
```

### Access Cached Content

```python
import hg_localization

model_id = "bert-base-uncased"

# Get model card content
card_content = hg_localization.get_cached_model_card_content(model_id)
if card_content:
    print("Model Card:")
    print(card_content[:200] + "...")

# Get model config content
config_content = hg_localization.get_cached_model_config_content(model_id)
if config_content:
    print(f"Model Type: {config_content.get('model_type')}")
    print(f"Architecture: {config_content.get('architectures', [])}")
    print(f"Hidden Size: {config_content.get('hidden_size')}")
```

### Fetch from Hugging Face

```python
import hg_localization

# Fetch model card directly from Hugging Face
card_content = hg_localization.get_model_card_content("bert-base-uncased")

# Fetch model config directly from Hugging Face  
config_content = hg_localization.get_model_config_content("bert-base-uncased")
```

## Storage Structure

Model metadata is stored in a similar structure to datasets:

```
models_store/
├── by_bucket/                    # Bucket-specific storage (when S3 configured)
│   └── bucket_name_hash/
│       └── model_id/
│           └── revision/
│               ├── model_card.md
│               ├── config.json
│               └── .hg_localization_bucket_metadata.json
└── model_id/                    # Legacy structure (when no S3)
    └── revision/
        ├── model_card.md
        ├── config.json
        └── .hg_localization_bucket_metadata.json
```

## Configuration

Model storage uses the same S3 configuration as datasets. You can set the model store path using:

```bash
export HGLOC_MODELS_STORE_PATH="/path/to/models"
```

Or configure it programmatically:

```python
from hg_localization import HGLocalizationConfig

config = HGLocalizationConfig(
    models_store_path="/path/to/models",
    s3_bucket_name="my-bucket",
    # ... other S3 settings
)
```

## Dependencies

- **Metadata Only**: Only requires `huggingface_hub` (already included)
- **Full Model**: Requires `transformers` library for downloading model weights and tokenizers

```bash
# For full model support
pip install transformers
```

## Future Extensions

This lightweight implementation can be extended to support:

1. **Full Model Download**: Download complete model weights and tokenizers
2. **Model Versioning**: Better support for model revisions and tags
3. **Public Model Registry**: Public sharing of model metadata via S3
4. **Model Collections**: Group related models together
5. **Model Comparison**: Compare configurations across different models

## Examples

### Explore Popular Models

```bash
# Download metadata for popular models
python -m hg_localization.cli download-model bert-base-uncased --no-s3-upload
python -m hg_localization.cli download-model gpt2 --no-s3-upload
python -m hg_localization.cli download-model microsoft/DialoGPT-medium --no-s3-upload

# List what you have
python -m hg_localization.cli list-local-models

# Compare configurations
python -m hg_localization.cli show-model-config bert-base-uncased
python -m hg_localization.cli show-model-config gpt2
```

### Team Workflow

```bash
# Team member 1: Download and share model metadata
python -m hg_localization.cli download-model custom-org/custom-model --make-public

# Team member 2: Access shared metadata (if S3 configured)
python -m hg_localization.cli list-local-models
python -m hg_localization.cli show-model-card custom-org/custom-model
```

This model localization feature provides a foundation for more advanced model management capabilities while keeping the initial implementation lightweight and focused on essential metadata. 