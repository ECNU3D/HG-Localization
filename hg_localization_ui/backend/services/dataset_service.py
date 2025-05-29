import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional, Dict, Any

from models import DatasetInfo, DatasetPreview, CodeExample
from config import get_config_from_request, is_public_access_only
from hg_localization import (
    load_local_dataset,
    list_local_datasets,
    list_s3_datasets,
    get_cached_dataset_card_content,
    get_dataset_card_content,
    download_dataset
)
from hg_localization.dataset_manager import _get_dataset_path

def get_cached_datasets_service(config, public_only: bool, filter_by_bucket: bool = True) -> List[DatasetInfo]:
    """Get list of cached datasets"""
    datasets = list_local_datasets(config=config, public_access_only=public_only, filter_by_bucket=filter_by_bucket)
    return [
        DatasetInfo(
            dataset_id=ds["dataset_id"],
            config_name=ds.get("config_name"),
            revision=ds.get("revision"),
            path=ds.get("path"),
            has_card=ds.get("has_card", False),
            source="cached",
            is_cached=True,
            available_s3=False
        )
        for ds in datasets
    ]

def get_s3_datasets_service(config) -> List[DatasetInfo]:
    """Get list of S3 datasets"""
    datasets = list_s3_datasets(config=config)
    return [
        DatasetInfo(
            dataset_id=ds["dataset_id"],
            config_name=ds.get("config_name"),
            revision=ds.get("revision"),
            s3_card_url=ds.get("s3_card_url"),
            has_card=bool(ds.get("s3_card_url")),
            source="s3",
            is_cached=False,
            available_s3=True
        )
        for ds in datasets
    ]

def get_all_datasets_service(config) -> List[DatasetInfo]:
    """Get combined list of cached and S3 datasets"""
    # Get cached datasets that match the current bucket configuration
    try:
        public_only = is_public_access_only(config)
        cached_datasets = get_cached_datasets_service(config, public_only, filter_by_bucket=True)
    except Exception:
        cached_datasets = []
    
    # Get S3 datasets
    s3_datasets = []
    if config and config.s3_bucket_name:
        try:
            s3_datasets = get_s3_datasets_service(config)
        except Exception:
            pass  # S3 might not be accessible
    
    # Combine and deduplicate
    all_datasets = {}
    
    # Add cached datasets
    for ds in cached_datasets:
        key = f"{ds.dataset_id}_{ds.config_name}_{ds.revision}"
        all_datasets[key] = ds
    
    # Add S3 datasets (mark as available in S3)
    for ds in s3_datasets:
        key = f"{ds.dataset_id}_{ds.config_name}_{ds.revision}"
        if key in all_datasets:
            # Dataset exists both cached and in S3
            existing = all_datasets[key]
            existing.available_s3 = True
            existing.source = "both"
            if ds.s3_card_url:
                existing.s3_card_url = ds.s3_card_url
            if not existing.has_card and ds.has_card:
                existing.has_card = ds.has_card
        else:
            # Dataset only in S3
            all_datasets[key] = ds
    
    return list(all_datasets.values())

def create_dataset_zip(dataset_id: str, config_name: Optional[str], revision: Optional[str], config, public_only: bool) -> Path:
    """Create a ZIP file for a dataset"""
    # First ensure the dataset is cached locally
    dataset = load_local_dataset(dataset_id, config_name, revision, config=config, public_access_only=public_only)
    if not dataset:
        raise ValueError("Dataset not found in cache. Please cache it first.")
    
    # Get the dataset path - check both public and private paths if needed
    if public_only:
        dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=True)
    else:
        # Check public path first, then private
        public_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=True)
        private_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=False)
        dataset_path = public_path if public_path.exists() else private_path
    
    if not dataset_path.exists():
        raise ValueError("Dataset files not found")
    
    # Create a temporary ZIP file
    temp_dir = tempfile.mkdtemp()
    safe_dataset_id = dataset_id.replace('/', '_')
    config_suffix = f"_{config_name}" if config_name else ""
    revision_suffix = f"_{revision}" if revision else ""
    zip_filename = f"{safe_dataset_id}{config_suffix}{revision_suffix}.zip"
    zip_path = Path(temp_dir) / zip_filename
    
    # Create ZIP file
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in dataset_path.rglob('*'):
            if file_path.is_file():
                # Add file to ZIP with relative path
                arcname = file_path.relative_to(dataset_path)
                zipf.write(file_path, arcname)
    
    return zip_path

def get_dataset_preview_service(dataset_id: str, config_name: Optional[str], revision: Optional[str], 
                               max_samples: int, config, public_only: bool) -> DatasetPreview:
    """Get dataset preview with sample data"""
    dataset = load_local_dataset(dataset_id, config_name, revision, config=config, public_access_only=public_only)
    if not dataset:
        raise ValueError("Dataset not found locally")
    
    # Handle both Dataset and DatasetDict
    if hasattr(dataset, 'features'):
        # Single Dataset
        features = dict(dataset.features)
        num_rows = {"train": len(dataset)}
        sample_data = [dict(dataset[i]) for i in range(min(max_samples, len(dataset)))]
    else:
        # DatasetDict
        split_name = list(dataset.keys())[0]  # Use first split
        split_dataset = dataset[split_name]
        features = dict(split_dataset.features)
        num_rows = {split: len(dataset[split]) for split in dataset.keys()}
        sample_data = [dict(split_dataset[i]) for i in range(min(max_samples, len(split_dataset)))]
    
    return DatasetPreview(
        dataset_id=dataset_id,
        config_name=config_name,
        revision=revision,
        features=features,
        num_rows=num_rows,
        sample_data=sample_data
    )

def get_dataset_card_service(dataset_id: str, config_name: Optional[str], revision: Optional[str], 
                           try_huggingface: bool, config) -> str:
    """Get dataset card content"""
    # Determine if we're in public access mode
    from config import is_public_access_only
    public_only = is_public_access_only(config)
    
    # First try to get cached card content (local cache and S3)
    card_content = get_cached_dataset_card_content(dataset_id, config_name, revision, config=config, public_access_only=public_only)
    
    # Only try Hugging Face if explicitly requested and no cached content found
    if not card_content and try_huggingface:
        print(f"Attempting to fetch dataset card from Hugging Face for {dataset_id}")
        card_content = get_dataset_card_content(dataset_id, revision)
    
    if not card_content:
        raise ValueError("Dataset card not found")
    
    return card_content

def get_dataset_examples_service(dataset_id: str, config_name: Optional[str], revision: Optional[str]) -> List[CodeExample]:
    """Get code examples for using the dataset"""
    examples = []
    
    # Convert dataset_id back to original format for Hugging Face operations
    original_dataset_id = dataset_id.replace('_', '/')
    
    # Basic loading example
    config_part = f", name='{config_name}'" if config_name else ""
    revision_part = f", revision='{revision}'" if revision else ""
    
    basic_code = f"""from hg_localization import load_local_dataset

# Load the dataset from local cache
dataset = load_local_dataset(
    dataset_id='{original_dataset_id}'{config_part}{revision_part}
)

if dataset:
    print(f"Dataset loaded successfully!")
    print(f"Available splits: {{list(dataset.keys())}}")
else:
    print("Dataset not found in local cache")"""
    
    examples.append(CodeExample(
        title="Load Dataset",
        description="Load the dataset from local cache using hg_localization",
        code=basic_code
    ))
    
    # Download example
    download_code = f"""from hg_localization import download_dataset

# Download dataset from Hugging Face and cache locally
success, path = download_dataset(
    dataset_id='{original_dataset_id}'{config_part}{revision_part},
    trust_remote_code=False
)

if success:
    print(f"Dataset downloaded to: {{path}}")
else:
    print(f"Download failed: {{path}}")"""
    
    examples.append(CodeExample(
        title="Download Dataset",
        description="Download the dataset from Hugging Face Hub",
        code=download_code
    ))
    
    # Direct Hugging Face usage
    hf_code = f"""from datasets import load_dataset

# Load directly from Hugging Face Hub
dataset = load_dataset('{original_dataset_id}'{config_part}{revision_part})

# Access different splits
train_data = dataset['train']
print(f"Training samples: {{len(train_data)}}")"""
    
    examples.append(CodeExample(
        title="Direct Hugging Face Usage",
        description="Load the dataset directly from Hugging Face Hub",
        code=hf_code
    ))
    
    # Load from downloaded ZIP file example
    zip_code = f"""from datasets import load_dataset
import zipfile
import tempfile
import os
from pathlib import Path

# Path to your downloaded ZIP file
zip_path = "{dataset_id.replace('/', '_')}{f'_{config_name}' if config_name else ''}{f'_{revision}' if revision else ''}.zip"

# Check if ZIP file exists
if not os.path.exists(zip_path):
    print(f"ZIP file not found: {{zip_path}}")
    print("Please download the dataset first using the 'Download ZIP' button in the UI")
else:
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Extracting {{zip_path}}...")
        
        # Extract ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # List extracted files to understand structure
        extracted_files = list(Path(temp_dir).rglob('*'))
        print(f"Extracted files: {{[f.name for f in extracted_files if f.is_file()]}}")
        
        # Auto-detect file format and load accordingly
        parquet_files = list(Path(temp_dir).rglob('*.parquet'))
        json_files = list(Path(temp_dir).rglob('*.json'))
        csv_files = list(Path(temp_dir).rglob('*.csv'))
        
        if parquet_files:
            # Load from Parquet files
            dataset = load_dataset('parquet', data_dir=temp_dir)
        elif json_files:
            # Load from JSON files
            dataset = load_dataset('json', data_dir=temp_dir)
        elif csv_files:
            # Load from CSV files
            dataset = load_dataset('csv', data_dir=temp_dir)
        else:
            # Try generic approach
            dataset = load_dataset(temp_dir)
        
        print(f"Dataset loaded from ZIP file!")
        print(f"Available splits: {{list(dataset.keys())}}")
        
        # Access the data
        if 'train' in dataset:
            train_data = dataset['train']
            print(f"Training samples: {{len(train_data)}}")
            print(f"Features: {{list(train_data.features.keys())}}")"""
    
    examples.append(CodeExample(
        title="Load from Downloaded ZIP",
        description="Load the dataset from a downloaded ZIP file using Hugging Face datasets",
        code=zip_code
    ))
    
    # Alternative ZIP loading method with manual file handling
    zip_alt_code = f"""from datasets import Dataset, DatasetDict
import zipfile
import pandas as pd
import json
import tempfile
import os
from pathlib import Path

# Method 2: Manual file handling from ZIP
zip_path = "{dataset_id.replace('/', '_')}{f'_{config_name}' if config_name else ''}{f'_{revision}' if revision else ''}.zip"

if not os.path.exists(zip_path):
    print(f"ZIP file not found: {{zip_path}}")
else:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract ZIP file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Find all data files
        data_files = {{
            'parquet': list(Path(temp_dir).rglob('*.parquet')),
            'json': list(Path(temp_dir).rglob('*.json')),
            'csv': list(Path(temp_dir).rglob('*.csv'))
        }}
        
        datasets = {{}}
        
        # Load Parquet files
        for pq_file in data_files['parquet']:
            split_name = pq_file.stem  # Use filename as split name
            df = pd.read_parquet(pq_file)
            datasets[split_name] = Dataset.from_pandas(df)
            print(f"Loaded {{split_name}}: {{len(df)}} samples")
        
        # Load JSON files (if no Parquet files found)
        if not data_files['parquet'] and data_files['json']:
            for json_file in data_files['json']:
                split_name = json_file.stem
                with open(json_file, 'r', encoding='utf-8') as f:
                    # Handle both JSON Lines and regular JSON
                    try:
                        data = json.load(f)  # Regular JSON
                        if isinstance(data, list):
                            datasets[split_name] = Dataset.from_list(data)
                    except:
                        # JSON Lines format
                        f.seek(0)
                        data = [json.loads(line) for line in f if line.strip()]
                        datasets[split_name] = Dataset.from_list(data)
                print(f"Loaded {{split_name}}: {{len(datasets[split_name])}} samples")
        
        # Load CSV files (if no other formats found)
        if not data_files['parquet'] and not data_files['json'] and data_files['csv']:
            for csv_file in data_files['csv']:
                split_name = csv_file.stem
                df = pd.read_csv(csv_file)
                datasets[split_name] = Dataset.from_pandas(df)
                print(f"Loaded {{split_name}}: {{len(df)}} samples")
        
        # Create final dataset
        if len(datasets) == 1:
            dataset = list(datasets.values())[0]
            print(f"Single dataset: {{len(dataset)}} samples")
        else:
            dataset = DatasetDict(datasets)
            print(f"Dataset dict with splits: {{list(dataset.keys())}}")
        
        # Show features
        if hasattr(dataset, 'features'):
            print(f"Features: {{list(dataset.features.keys())}}")
        else:
            first_split = list(dataset.keys())[0]
            print(f"Features: {{list(dataset[first_split].features.keys())}}")"""
    
    examples.append(CodeExample(
        title="Advanced ZIP Loading",
        description="Comprehensive method to load datasets from ZIP with auto-detection of file formats (Parquet, JSON, CSV)",
        code=zip_alt_code
    ))
    
    return examples

async def cache_dataset_task(dataset_id: str, config_name: Optional[str], revision: Optional[str], 
                           trust_remote_code: bool, make_public: bool, config, manager):
    """Background task for caching a dataset"""
    try:
        await manager.broadcast(f"Starting caching of {dataset_id}...")
        
        success, message = download_dataset(
            dataset_id=dataset_id,
            config_name=config_name,
            revision=revision,
            trust_remote_code=trust_remote_code,
            make_public=make_public,
            config=config,
            skip_hf_card_fetch=True  # Skip HF card fetching in isolated server environment
        )
        
        if success:
            await manager.broadcast(f"Successfully cached {dataset_id}")
        else:
            await manager.broadcast(f"Failed to cache {dataset_id}: {message}")
            
    except Exception as e:
        await manager.broadcast(f"Error caching {dataset_id}: {str(e)}") 