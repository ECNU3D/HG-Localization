from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request

from models import ModelInfo, ModelDownloadRequest, ModelCard, ModelConfig, CodeExample
from config import get_config_from_request, is_public_access_only
from websocket_manager import manager
from services.model_service import (
    get_cached_models_service,
    get_s3_models_service,
    get_all_models_service,
    get_model_card_service,
    get_model_config_service,
    get_model_examples_service,
    cache_model_task
)

router = APIRouter(prefix="/api/models", tags=["models"])

@router.get("/cached", response_model=List[ModelInfo])
async def get_cached_models(request: Request):
    """Get list of cached models that match the current S3 bucket configuration"""
    try:
        config = get_config_from_request(request)
        public_only = is_public_access_only(config)
        return get_cached_models_service(config, public_only, filter_by_bucket=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing cached models: {str(e)}")

@router.get("/s3", response_model=List[ModelInfo])
async def get_s3_models(request: Request):
    """Get list of S3 models"""
    config = get_config_from_request(request)
    if not config or not config.s3_bucket_name:
        raise HTTPException(status_code=400, detail="S3 bucket not configured")
    
    try:
        return get_s3_models_service(config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing S3 models: {str(e)}")

@router.get("/all", response_model=List[ModelInfo])
async def get_all_models(request: Request):
    """Get combined list of cached and S3 models that match the current bucket configuration"""
    config = get_config_from_request(request)
    return get_all_models_service(config)

@router.post("/cache")
async def cache_model_endpoint(request: ModelDownloadRequest, background_tasks: BackgroundTasks, req: Request):
    """Cache a model on the server"""
    config = get_config_from_request(req)
    
    if not config or (not config.s3_bucket_name and request.make_public):
        raise HTTPException(status_code=400, detail="S3 bucket required for making models public")
    
    background_tasks.add_task(
        cache_model_task,
        request.model_id,
        request.revision,
        request.make_public,
        request.metadata_only,
        config,
        manager
    )
    return {"message": "Caching started", "model_id": request.model_id}

@router.get("/{model_id:path}/card")
async def get_model_card(
    model_id: str,
    revision: Optional[str] = None,
    try_huggingface: bool = False,
    request: Request = None
):
    """Get model card content"""
    try:
        config = get_config_from_request(request)
        card_content = get_model_card_service(model_id, revision, try_huggingface, config)
        return ModelCard(content=card_content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching model card: {str(e)}")

@router.get("/{model_id:path}/config")
async def get_model_config(
    model_id: str,
    revision: Optional[str] = None,
    try_huggingface: bool = False,
    request: Request = None
):
    """Get model config content"""
    try:
        config = get_config_from_request(request)
        config_content = get_model_config_service(model_id, revision, try_huggingface, config)
        return ModelConfig(config=config_content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching model config: {str(e)}")

@router.get("/{model_id:path}/examples", response_model=List[CodeExample])
async def get_model_examples(
    model_id: str,
    revision: Optional[str] = None
):
    """Get code examples for using the model"""
    return get_model_examples_service(model_id, revision) 