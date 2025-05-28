from typing import Optional
from fastapi import APIRouter, HTTPException, Request

from models import MigrationStatus, MigrationResult
from config import get_config_from_request
from services.migration_service import (
    get_migration_status_service,
    migrate_all_datasets_service,
    migrate_single_dataset_service
)

router = APIRouter(prefix="/api/datasets/migration", tags=["migration"])

@router.get("/status", response_model=MigrationStatus)
async def get_migration_status(request: Request):
    """Check if there are legacy datasets that need migration to bucket-specific storage"""
    try:
        config = get_config_from_request(request)
        return get_migration_status_service(config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking migration status: {str(e)}")

@router.post("/migrate-all", response_model=MigrationResult)
async def migrate_all_datasets(request: Request):
    """Migrate all legacy datasets to bucket-specific storage"""
    try:
        config = get_config_from_request(request)
        return migrate_all_datasets_service(config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during migration: {str(e)}")

@router.post("/{dataset_id:path}/migrate")
async def migrate_single_dataset(
    dataset_id: str,
    config_name: Optional[str] = None,
    revision: Optional[str] = None,
    request: Request = None
):
    """Migrate a single dataset to bucket-specific storage"""
    try:
        config = get_config_from_request(request)
        success = migrate_single_dataset_service(dataset_id, config_name, revision, config)
        
        if success:
            return {"message": f"Successfully migrated dataset {dataset_id}"}
        else:
            raise HTTPException(status_code=500, detail=f"Failed to migrate dataset {dataset_id}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error migrating dataset: {str(e)}") 