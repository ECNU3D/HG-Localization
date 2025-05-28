import os
import sys
import asyncio
import json
import base64
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
import uvicorn
import tempfile
import zipfile
import shutil
from pathlib import Path

# Add the parent directory to sys.path to import hg_localization
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from hg_localization import (
    HGLocalizationConfig,
    download_dataset,
    load_local_dataset,
    list_local_datasets,
    list_s3_datasets,
    get_cached_dataset_card_content,
    get_dataset_card_content,
    sync_local_dataset_to_s3,
    upload_dataset
)

# Import model management functions
from hg_localization.model_manager import (
    download_model_metadata,
    list_local_models,
    get_cached_model_card_content,
    get_cached_model_config_content,
    get_model_card_content,
    get_model_config_content
)

# Import migration functions
from hg_localization.dataset_manager import (
    migrate_all_datasets_to_bucket_storage,
    migrate_dataset_to_bucket_storage,
    _get_legacy_dataset_path,
    _get_dataset_path
)

# Cookie configuration
COOKIE_NAME = "hg_localization_config"
COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 days in seconds

# WebSocket connection manager for real-time updates
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting HG-Localization UI Backend...")
    yield
    # Shutdown
    print("Shutting down HG-Localization UI Backend...")

app = FastAPI(
    title="HG-Localization UI API",
    description="API for managing Hugging Face datasets with S3 integration",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class S3Config(BaseModel):
    s3_bucket_name: str = Field(..., description="S3 bucket name")
    s3_endpoint_url: Optional[str] = Field(None, description="S3 endpoint URL for S3-compatible services")
    aws_access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(None, description="AWS secret access key")
    s3_data_prefix: Optional[str] = Field("", description="S3 data prefix")

class ConfigStatus(BaseModel):
    configured: bool
    has_credentials: bool
    bucket_name: Optional[str]
    endpoint_url: Optional[str]
    data_prefix: Optional[str]

class DatasetInfo(BaseModel):
    dataset_id: str
    config_name: Optional[str]
    revision: Optional[str]
    path: Optional[str] = None
    has_card: bool = False
    s3_card_url: Optional[str] = None
    source: str  # "cached", "s3", or "both"
    is_cached: bool = False
    available_s3: bool = False

class DatasetDownloadRequest(BaseModel):
    dataset_id: str
    config_name: Optional[str] = None
    revision: Optional[str] = None
    trust_remote_code: bool = False
    make_public: bool = False

class DatasetPreview(BaseModel):
    dataset_id: str
    config_name: Optional[str]
    revision: Optional[str]
    features: Dict[str, Any]
    num_rows: Dict[str, int]
    sample_data: List[Dict[str, Any]]

class ModelInfo(BaseModel):
    model_id: str
    revision: Optional[str]
    path: Optional[str] = None
    has_card: bool = False
    has_config: bool = False
    has_tokenizer: bool = False
    is_full_model: bool = False
    source: str  # "cached", "s3", or "both"
    is_cached: bool = False
    available_s3: bool = False

class ModelDownloadRequest(BaseModel):
    model_id: str
    revision: Optional[str] = None
    make_public: bool = False
    metadata_only: bool = True

class ModelCard(BaseModel):
    content: str

class ModelConfig(BaseModel):
    config: Dict[str, Any]

class CodeExample(BaseModel):
    title: str
    description: str
    code: str
    language: str = "python"

def encode_config_cookie(config: S3Config) -> str:
    """Encode configuration as a base64 cookie value"""
    config_dict = config.model_dump()
    config_json = json.dumps(config_dict)
    return base64.b64encode(config_json.encode()).decode()

def decode_config_cookie(cookie_value: str) -> Optional[S3Config]:
    """Decode configuration from a base64 cookie value"""
    try:
        config_json = base64.b64decode(cookie_value.encode()).decode()
        config_dict = json.loads(config_json)
        return S3Config(**config_dict)
    except Exception:
        return None

def get_config_from_request(request: Request) -> Optional[HGLocalizationConfig]:
    """Extract configuration from request cookies and create HGLocalizationConfig"""
    cookie_value = request.cookies.get(COOKIE_NAME)
    if not cookie_value:
        return None
    
    s3_config = decode_config_cookie(cookie_value)
    if not s3_config:
        return None
    
    return HGLocalizationConfig(
        s3_bucket_name=s3_config.s3_bucket_name,
        s3_endpoint_url=s3_config.s3_endpoint_url,
        aws_access_key_id=s3_config.aws_access_key_id,
        aws_secret_access_key=s3_config.aws_secret_access_key,
        s3_data_prefix=s3_config.s3_data_prefix or ""
    )

def is_public_access_only(config: Optional[HGLocalizationConfig]) -> bool:
    """Determine if this is public access only (no credentials provided)"""
    return config is None or not config.has_credentials()

def get_config_status_from_config(config: Optional[HGLocalizationConfig]) -> ConfigStatus:
    """Get configuration status from HGLocalizationConfig object"""
    if not config:
        return ConfigStatus(
            configured=False,
            has_credentials=False,
            bucket_name=None,
            endpoint_url=None,
            data_prefix=None
        )
    
    return ConfigStatus(
        configured=bool(config.s3_bucket_name),
        has_credentials=config.has_credentials(),
        bucket_name=config.s3_bucket_name,
        endpoint_url=config.s3_endpoint_url,
        data_prefix=config.s3_data_prefix
    )

# Configuration endpoints
@app.post("/api/config", response_model=ConfigStatus)
async def set_config(config: S3Config, response: Response):
    """Set S3 configuration via cookie"""
    # Ensure credentials are None if not provided (for public access)
    aws_access_key_id = config.aws_access_key_id if config.aws_access_key_id else None
    aws_secret_access_key = config.aws_secret_access_key if config.aws_secret_access_key else None
    
    # Create a clean config for cookie storage
    clean_config = S3Config(
        s3_bucket_name=config.s3_bucket_name,
        s3_endpoint_url=config.s3_endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        s3_data_prefix=config.s3_data_prefix
    )
    
    # Encode and set cookie
    cookie_value = encode_config_cookie(clean_config)
    response.set_cookie(
        key=COOKIE_NAME,
        value=cookie_value,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=False,  # Set to True in production with HTTPS
        samesite="lax"
    )
    
    # Create HGLocalizationConfig for status response
    hg_config = HGLocalizationConfig(
        s3_bucket_name=clean_config.s3_bucket_name,
        s3_endpoint_url=clean_config.s3_endpoint_url,
        aws_access_key_id=clean_config.aws_access_key_id,
        aws_secret_access_key=clean_config.aws_secret_access_key,
        s3_data_prefix=clean_config.s3_data_prefix or ""
    )
    
    return get_config_status_from_config(hg_config)

@app.get("/api/config/status", response_model=ConfigStatus)
async def get_config_status(request: Request):
    """Get current configuration status from cookie"""
    config = get_config_from_request(request)
    return get_config_status_from_config(config)

# Dataset endpoints
@app.get("/api/datasets/cached", response_model=List[DatasetInfo])
async def get_cached_datasets(request: Request):
    """Get list of cached datasets that match the current S3 bucket configuration"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        # Filter datasets by bucket configuration to only show datasets from the current bucket
        datasets = list_local_datasets(config=config, public_access_only=public_only, filter_by_bucket=True)
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing cached datasets: {str(e)}")

@app.get("/api/datasets/cached/all", response_model=List[DatasetInfo])
async def get_all_cached_datasets(request: Request):
    """Get list of all cached datasets regardless of bucket configuration (for debugging/migration)"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        # Don't filter by bucket - show all cached datasets
        datasets = list_local_datasets(config=config, public_access_only=public_only, filter_by_bucket=False)
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing all cached datasets: {str(e)}")

@app.get("/api/datasets/s3", response_model=List[DatasetInfo])
async def get_s3_datasets(request: Request):
    """Get list of S3 datasets"""
    config = get_config_from_request(request)
    if not config or not config.s3_bucket_name:
        raise HTTPException(status_code=400, detail="S3 bucket not configured")
    
    try:
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing S3 datasets: {str(e)}")

@app.get("/api/datasets/all", response_model=List[DatasetInfo])
async def get_all_datasets(request: Request):
    """Get combined list of cached and S3 datasets that match the current bucket configuration"""
    config = get_config_from_request(request)
    
    # Get cached datasets that match the current bucket configuration
    try:
        public_only = is_public_access_only(config)
        # Filter datasets by bucket configuration to only show datasets from the current bucket
        cached_datasets_raw = list_local_datasets(config=config, public_access_only=public_only, filter_by_bucket=True)
        cached_datasets = [
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
            for ds in cached_datasets_raw
        ]
    except Exception:
        cached_datasets = []
    
    # Get S3 datasets
    s3_datasets = []
    if config and config.s3_bucket_name:
        try:
            s3_datasets_raw = list_s3_datasets(config=config)
            s3_datasets = [
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
                for ds in s3_datasets_raw
            ]
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

@app.post("/api/datasets/cache")
async def cache_dataset_endpoint(request: DatasetDownloadRequest, background_tasks: BackgroundTasks, req: Request):
    """Cache a dataset on the server"""
    config = get_config_from_request(req)
    
    if not config or (not config.s3_bucket_name and request.make_public):
        raise HTTPException(status_code=400, detail="S3 bucket required for making datasets public")
    
    async def cache_task():
        try:
            await manager.broadcast(f"Starting caching of {request.dataset_id}...")
            
            success, message = download_dataset(
                dataset_id=request.dataset_id,
                config_name=request.config_name,
                revision=request.revision,
                trust_remote_code=request.trust_remote_code,
                make_public=request.make_public,
                config=config,
                skip_hf_card_fetch=True  # Skip HF card fetching in isolated server environment
            )
            
            if success:
                await manager.broadcast(f"Successfully cached {request.dataset_id}")
            else:
                await manager.broadcast(f"Failed to cache {request.dataset_id}: {message}")
                
        except Exception as e:
            await manager.broadcast(f"Error caching {request.dataset_id}: {str(e)}")
    
    background_tasks.add_task(cache_task)
    return {"message": "Caching started", "dataset_id": request.dataset_id}

@app.get("/api/datasets/{dataset_id:path}/download")
async def download_dataset_zip(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None,
    request: Request = None
):
    """Download a dataset as a ZIP file to user's computer"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        
        # First ensure the dataset is cached locally
        dataset = load_local_dataset(dataset_id, config_name, revision, config=config, public_access_only=public_only)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found in cache. Please cache it first.")
        
        # Get the dataset path - check both public and private paths if needed
        from hg_localization.dataset_manager import _get_dataset_path
        if public_only:
            dataset_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=True)
        else:
            # Check public path first, then private
            public_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=True)
            private_path = _get_dataset_path(dataset_id, config_name, revision, config=config, is_public=False)
            dataset_path = public_path if public_path.exists() else private_path
        
        if not dataset_path.exists():
            raise HTTPException(status_code=404, detail="Dataset files not found")
        
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
        
        # Return the ZIP file as a download
        return FileResponse(
            path=str(zip_path),
            filename=zip_filename,
            media_type='application/zip',
            background=BackgroundTasks()  # This will clean up the temp file after sending
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating dataset ZIP: {str(e)}")

@app.get("/api/datasets/{dataset_id:path}/preview")
async def get_dataset_preview(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None,
    max_samples: int = 5,
    request: Request = None
):
    """Get dataset preview with sample data"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        dataset = load_local_dataset(dataset_id, config_name, revision, config=config, public_access_only=public_only)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found locally")
        
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error previewing dataset: {str(e)}")

@app.get("/api/datasets/{dataset_id:path}/card")
async def get_dataset_card(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None,
    try_huggingface: bool = False,
    request: Request = None
):
    """Get dataset card content"""
    try:
        config = get_config_from_request(request)
        
        # First try to get cached card content (local cache and S3)
        card_content = get_cached_dataset_card_content(dataset_id, config_name, revision, config=config)
        
        # Only try Hugging Face if explicitly requested and no cached content found
        if not card_content and try_huggingface:
            print(f"Attempting to fetch dataset card from Hugging Face for {dataset_id}")
            card_content = get_dataset_card_content(dataset_id, revision)
        
        if not card_content:
            raise HTTPException(status_code=404, detail="Dataset card not found")
        
        return {"content": card_content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dataset card: {str(e)}")

@app.get("/api/datasets/{dataset_id:path}/examples", response_model=List[CodeExample])
async def get_dataset_examples(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None
):
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

# Model endpoints
@app.get("/api/models/cached", response_model=List[ModelInfo])
async def get_cached_models(request: Request):
    """Get list of cached models that match the current S3 bucket configuration"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        # Filter models by bucket configuration to only show models from the current bucket
        models = list_local_models(config=config, public_access_only=public_only, filter_by_bucket=True)
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing cached models: {str(e)}")

@app.post("/api/models/cache")
async def cache_model_endpoint(request: ModelDownloadRequest, background_tasks: BackgroundTasks, req: Request):
    """Cache a model on the server"""
    config = get_config_from_request(req)
    
    if not config or (not config.s3_bucket_name and request.make_public):
        raise HTTPException(status_code=400, detail="S3 bucket required for making models public")
    
    async def cache_task():
        try:
            download_type = "metadata" if request.metadata_only else "full model"
            await manager.broadcast(f"Starting caching of {download_type} for {request.model_id}...")
            
            success, message = download_model_metadata(
                model_id=request.model_id,
                revision=request.revision,
                make_public=request.make_public,
                config=config,
                skip_hf_fetch=True,  # Skip HF fetch in isolated server environment
                metadata_only=request.metadata_only
            )
            
            if success:
                await manager.broadcast(f"Successfully cached {download_type} for {request.model_id}")
            else:
                await manager.broadcast(f"Failed to cache {download_type} for {request.model_id}: {message}")
                
        except Exception as e:
            await manager.broadcast(f"Error caching {request.model_id}: {str(e)}")
    
    background_tasks.add_task(cache_task)
    return {"message": "Caching started", "model_id": request.model_id}

@app.get("/api/models/{model_id:path}/card")
async def get_model_card(
    model_id: str,
    revision: Optional[str] = None,
    try_huggingface: bool = False,
    request: Request = None
):
    """Get model card content"""
    try:
        config = get_config_from_request(request)
        
        # First try to get cached card content (local cache and S3)
        card_content = get_cached_model_card_content(model_id, revision, config=config)
        
        # Only try Hugging Face if explicitly requested and no cached content found
        if not card_content and try_huggingface:
            print(f"Attempting to fetch model card from Hugging Face for {model_id}")
            card_content = get_model_card_content(model_id, revision)
        
        if not card_content:
            raise HTTPException(status_code=404, detail="Model card not found")
        
        return ModelCard(content=card_content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching model card: {str(e)}")

@app.get("/api/models/{model_id:path}/config")
async def get_model_config(
    model_id: str,
    revision: Optional[str] = None,
    try_huggingface: bool = False,
    request: Request = None
):
    """Get model config content"""
    try:
        config = get_config_from_request(request)
        
        # First try to get cached config content (local cache and S3)
        config_content = get_cached_model_config_content(model_id, revision, config=config)
        
        # Only try Hugging Face if explicitly requested and no cached content found
        if not config_content and try_huggingface:
            print(f"Attempting to fetch model config from Hugging Face for {model_id}")
            config_content = get_model_config_content(model_id, revision)
        
        if not config_content:
            raise HTTPException(status_code=404, detail="Model config not found")
        
        return ModelConfig(config=config_content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching model config: {str(e)}")

@app.get("/api/models/{model_id:path}/examples", response_model=List[CodeExample])
async def get_model_examples(
    model_id: str,
    revision: Optional[str] = None
):
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

# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Echo back for now, can be extended for specific commands
            await manager.send_personal_message(f"Echo: {data}", websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Health check
@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "service": "hg-localization-ui"}

# Serve static files (for production)
if Path("static").exists():
    app.mount("/", StaticFiles(directory="static", html=True), name="static")

class MigrationStatus(BaseModel):
    needs_migration: bool
    legacy_datasets_count: int
    current_bucket: Optional[str]
    migration_available: bool

class MigrationResult(BaseModel):
    success: bool
    migrated_count: int
    failed_count: int
    message: str

@app.get("/api/datasets/migration/status", response_model=MigrationStatus)
async def get_migration_status(request: Request):
    """Check if there are legacy datasets that need migration to bucket-specific storage"""
    try:
        config = get_config_from_request(request)
        
        # Check if migration is available (requires S3 bucket configuration)
        migration_available = config is not None and config.s3_bucket_name is not None
        
        if not migration_available:
            return MigrationStatus(
                needs_migration=False,
                legacy_datasets_count=0,
                current_bucket=None,
                migration_available=False
            )
        
        # Get all datasets without bucket filtering to find legacy ones
        all_datasets = list_local_datasets(config=config, filter_by_bucket=False)
        
        # Count datasets that are in legacy storage locations
        legacy_count = 0
        for ds in all_datasets:
            dataset_id = ds["dataset_id"]
            config_name = ds.get("config_name")
            revision = ds.get("revision")
            is_public = ds.get("is_public", False)
            
            # Check if this dataset is in a legacy path
            legacy_path = _get_legacy_dataset_path(dataset_id, config_name, revision, config, is_public)
            current_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public)
            
            # If the dataset path equals the legacy path, it needs migration
            if str(legacy_path) == ds["path"] and str(legacy_path) != str(current_path):
                legacy_count += 1
        
        return MigrationStatus(
            needs_migration=legacy_count > 0,
            legacy_datasets_count=legacy_count,
            current_bucket=config.s3_bucket_name,
            migration_available=migration_available
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking migration status: {str(e)}")

@app.post("/api/datasets/migration/migrate-all", response_model=MigrationResult)
async def migrate_all_datasets(request: Request):
    """Migrate all legacy datasets to bucket-specific storage"""
    try:
        config = get_config_from_request(request)
        
        if not config or not config.s3_bucket_name:
            raise HTTPException(status_code=400, detail="S3 bucket configuration required for migration")
        
        # Get datasets before migration
        datasets_before = list_local_datasets(config=config, filter_by_bucket=False)
        legacy_datasets = []
        
        for ds in datasets_before:
            dataset_id = ds["dataset_id"]
            config_name = ds.get("config_name")
            revision = ds.get("revision")
            is_public = ds.get("is_public", False)
            
            # Check if this dataset is in a legacy path
            legacy_path = _get_legacy_dataset_path(dataset_id, config_name, revision, config, is_public)
            current_path = _get_dataset_path(dataset_id, config_name, revision, config, is_public)
            
            if str(legacy_path) == ds["path"] and str(legacy_path) != str(current_path):
                legacy_datasets.append((dataset_id, config_name, revision, is_public))
        
        # Perform migration
        migrated_count = 0
        failed_count = 0
        
        for dataset_id, config_name, revision, is_public in legacy_datasets:
            if migrate_dataset_to_bucket_storage(dataset_id, config_name, revision, config, is_public):
                migrated_count += 1
            else:
                failed_count += 1
        
        return MigrationResult(
            success=failed_count == 0,
            migrated_count=migrated_count,
            failed_count=failed_count,
            message=f"Migration completed: {migrated_count} datasets migrated, {failed_count} failed"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during migration: {str(e)}")

@app.post("/api/datasets/{dataset_id:path}/migrate")
async def migrate_single_dataset(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None,
    request: Request = None
):
    """Migrate a single dataset to bucket-specific storage"""
    try:
        config = get_config_from_request(request)
        
        if not config or not config.s3_bucket_name:
            raise HTTPException(status_code=400, detail="S3 bucket configuration required for migration")
        
        # Determine if this is a public dataset by checking both paths
        is_public = False
        public_legacy_path = _get_legacy_dataset_path(dataset_id, config_name, revision, config, is_public=True)
        if public_legacy_path.exists():
            is_public = True
        
        success = migrate_dataset_to_bucket_storage(dataset_id, config_name, revision, config, is_public)
        
        if success:
            return {"message": f"Successfully migrated dataset {dataset_id}"}
        else:
            raise HTTPException(status_code=500, detail=f"Failed to migrate dataset {dataset_id}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error migrating dataset: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 