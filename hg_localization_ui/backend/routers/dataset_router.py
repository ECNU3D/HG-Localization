from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from fastapi.responses import FileResponse

from models import DatasetInfo, DatasetDownloadRequest, DatasetPreview, CodeExample
from config import get_config_from_request, is_public_access_only
from websocket_manager import manager
from services.dataset_service import (
    get_cached_datasets_service,
    get_s3_datasets_service,
    get_all_datasets_service,
    create_dataset_zip,
    get_dataset_preview_service,
    get_dataset_card_service,
    get_dataset_examples_service,
    cache_dataset_task
)

router = APIRouter(prefix="/api/datasets", tags=["datasets"])

@router.get("/cached", response_model=List[DatasetInfo])
async def get_cached_datasets(request: Request):
    """Get list of cached datasets that match the current S3 bucket configuration"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        return get_cached_datasets_service(config, public_only, filter_by_bucket=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing cached datasets: {str(e)}")

@router.get("/cached/all", response_model=List[DatasetInfo])
async def get_all_cached_datasets(request: Request):
    """Get list of all cached datasets regardless of bucket configuration (for debugging/migration)"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        return get_cached_datasets_service(config, public_only, filter_by_bucket=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing all cached datasets: {str(e)}")

@router.get("/s3", response_model=List[DatasetInfo])
async def get_s3_datasets(request: Request):
    """Get list of S3 datasets"""
    config = get_config_from_request(request)
    if not config or not config.s3_bucket_name:
        raise HTTPException(status_code=400, detail="S3 bucket not configured")
    
    try:
        return get_s3_datasets_service(config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing S3 datasets: {str(e)}")

@router.get("/all", response_model=List[DatasetInfo])
async def get_all_datasets(request: Request):
    """Get combined list of cached and S3 datasets that match the current bucket configuration"""
    config = get_config_from_request(request)
    return get_all_datasets_service(config)

@router.post("/cache")
async def cache_dataset_endpoint(request: DatasetDownloadRequest, background_tasks: BackgroundTasks, req: Request):
    """Cache a dataset on the server"""
    config = get_config_from_request(req)
    
    if not config or (not config.s3_bucket_name and request.make_public):
        raise HTTPException(status_code=400, detail="S3 bucket required for making datasets public")
    
    background_tasks.add_task(
        cache_dataset_task,
        request.dataset_id,
        request.config_name,
        request.revision,
        request.trust_remote_code,
        request.make_public,
        config,
        manager
    )
    return {"message": "Caching started", "dataset_id": request.dataset_id}

@router.get("/{dataset_id:path}/download")
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
        
        zip_path = create_dataset_zip(dataset_id, config_name, revision, config, public_only)
        
        # Return the ZIP file as a download
        return FileResponse(
            path=str(zip_path),
            filename=zip_path.name,
            media_type='application/zip',
            background=BackgroundTasks()  # This will clean up the temp file after sending
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating dataset ZIP: {str(e)}")

@router.get("/{dataset_id:path}/preview")
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
        return get_dataset_preview_service(dataset_id, config_name, revision, max_samples, config, public_only)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error previewing dataset: {str(e)}")

@router.get("/{dataset_id:path}/card")
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
        card_content = get_dataset_card_service(dataset_id, config_name, revision, try_huggingface, config)
        return {"content": card_content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dataset card: {str(e)}")

@router.get("/{dataset_id:path}/examples", response_model=List[CodeExample])
async def get_dataset_examples(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None
):
    """Get code examples for using the dataset"""
    return get_dataset_examples_service(dataset_id, config_name, revision) 