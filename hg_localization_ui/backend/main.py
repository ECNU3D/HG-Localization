import os
import sys
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
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
    download_dataset,
    load_local_dataset,
    list_local_datasets,
    list_s3_datasets,
    get_cached_dataset_card_content,
    get_dataset_card_content,
    sync_local_dataset_to_s3,
    upload_dataset
)

# Global configuration state
app_config = {
    "s3_bucket_name": None,
    "s3_endpoint_url": None,
    "aws_access_key_id": None,
    "aws_secret_access_key": None,
    "s3_data_prefix": "",
    "has_credentials": False
}

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

class CodeExample(BaseModel):
    title: str
    description: str
    code: str
    language: str = "python"

def update_environment_variables():
    """Update environment variables based on current app config"""
    if app_config["s3_bucket_name"]:
        os.environ["HGLOC_S3_BUCKET_NAME"] = app_config["s3_bucket_name"]
    
    if app_config["s3_endpoint_url"]:
        os.environ["HGLOC_S3_ENDPOINT_URL"] = app_config["s3_endpoint_url"]
    
    if app_config["aws_access_key_id"]:
        os.environ["HGLOC_AWS_ACCESS_KEY_ID"] = app_config["aws_access_key_id"]
    
    if app_config["aws_secret_access_key"]:
        os.environ["HGLOC_AWS_SECRET_ACCESS_KEY"] = app_config["aws_secret_access_key"]
    
    if app_config["s3_data_prefix"]:
        os.environ["HGLOC_S3_DATA_PREFIX"] = app_config["s3_data_prefix"]

# Configuration endpoints
@app.post("/api/config", response_model=ConfigStatus)
async def set_config(config: S3Config):
    """Set S3 configuration"""
    app_config["s3_bucket_name"] = config.s3_bucket_name
    app_config["s3_endpoint_url"] = config.s3_endpoint_url
    app_config["aws_access_key_id"] = config.aws_access_key_id
    app_config["aws_secret_access_key"] = config.aws_secret_access_key
    app_config["s3_data_prefix"] = config.s3_data_prefix or ""
    app_config["has_credentials"] = bool(config.aws_access_key_id and config.aws_secret_access_key)
    
    # Update environment variables for hg_localization
    update_environment_variables()
    
    # Reload the config module to pick up new environment variables
    import importlib
    import hg_localization.config
    importlib.reload(hg_localization.config)
    
    return ConfigStatus(
        configured=True,
        has_credentials=app_config["has_credentials"],
        bucket_name=app_config["s3_bucket_name"],
        endpoint_url=app_config["s3_endpoint_url"],
        data_prefix=app_config["s3_data_prefix"]
    )

@app.get("/api/config/status", response_model=ConfigStatus)
async def get_config_status():
    """Get current configuration status"""
    return ConfigStatus(
        configured=bool(app_config["s3_bucket_name"]),
        has_credentials=app_config["has_credentials"],
        bucket_name=app_config["s3_bucket_name"],
        endpoint_url=app_config["s3_endpoint_url"],
        data_prefix=app_config["s3_data_prefix"]
    )

# Dataset endpoints
@app.get("/api/datasets/cached", response_model=List[DatasetInfo])
async def get_cached_datasets():
    """Get list of cached datasets"""
    try:
        datasets = list_local_datasets()
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

@app.get("/api/datasets/s3", response_model=List[DatasetInfo])
async def get_s3_datasets():
    """Get list of S3 datasets"""
    if not app_config["s3_bucket_name"]:
        raise HTTPException(status_code=400, detail="S3 bucket not configured")
    
    try:
        datasets = list_s3_datasets()
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
async def get_all_datasets():
    """Get combined list of cached and S3 datasets"""
    cached_datasets = await get_cached_datasets()
    
    s3_datasets = []
    if app_config["s3_bucket_name"]:
        try:
            s3_datasets = await get_s3_datasets()
        except:
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
async def cache_dataset_endpoint(request: DatasetDownloadRequest, background_tasks: BackgroundTasks):
    """Cache a dataset on the server"""
    if not app_config["s3_bucket_name"] and request.make_public:
        raise HTTPException(status_code=400, detail="S3 bucket required for making datasets public")
    
    async def cache_task():
        try:
            await manager.broadcast(f"Starting caching of {request.dataset_id}...")
            
            success, message = download_dataset(
                dataset_id=request.dataset_id,
                config_name=request.config_name,
                revision=request.revision,
                trust_remote_code=request.trust_remote_code,
                make_public=request.make_public
            )
            
            if success:
                await manager.broadcast(f"Successfully cached {request.dataset_id}")
            else:
                await manager.broadcast(f"Failed to cache {request.dataset_id}: {message}")
                
        except Exception as e:
            await manager.broadcast(f"Error caching {request.dataset_id}: {str(e)}")
    
    background_tasks.add_task(cache_task)
    return {"message": "Caching started", "dataset_id": request.dataset_id}

@app.get("/api/datasets/{dataset_id}/download")
async def download_dataset_zip(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None
):
    """Download a dataset as a ZIP file to user's computer"""
    try:
        # First ensure the dataset is cached locally
        dataset = load_local_dataset(dataset_id, config_name, revision)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found in cache. Please cache it first.")
        
        # Get the dataset path
        from hg_localization.dataset_manager import _get_dataset_path
        dataset_path = _get_dataset_path(dataset_id, config_name, revision)
        
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

@app.get("/api/datasets/{dataset_id}/preview")
async def get_dataset_preview(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None,
    max_samples: int = 5
):
    """Get dataset preview with sample data"""
    try:
        dataset = load_local_dataset(dataset_id, config_name, revision)
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

@app.get("/api/datasets/{dataset_id}/card")
async def get_dataset_card(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None
):
    """Get dataset card content"""
    try:
        # Try to get cached card first
        card_content = get_cached_dataset_card_content(dataset_id, config_name, revision)
        
        if not card_content:
            # Try to fetch from Hugging Face
            card_content = get_dataset_card_content(dataset_id, revision)
        
        if not card_content:
            raise HTTPException(status_code=404, detail="Dataset card not found")
        
        return {"content": card_content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dataset card: {str(e)}")

@app.get("/api/datasets/{dataset_id}/examples", response_model=List[CodeExample])
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

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 