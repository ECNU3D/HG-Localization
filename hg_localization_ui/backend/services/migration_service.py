from typing import Optional

from models import MigrationStatus, MigrationResult
from hg_localization import list_local_datasets
from hg_localization.dataset_manager import (
    migrate_dataset_to_bucket_storage,
    _get_legacy_dataset_path,
    _get_dataset_path
)

def get_migration_status_service(config) -> MigrationStatus:
    """Check if there are legacy datasets that need migration to bucket-specific storage"""
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

def migrate_all_datasets_service(config) -> MigrationResult:
    """Migrate all legacy datasets to bucket-specific storage"""
    if not config or not config.s3_bucket_name:
        raise ValueError("S3 bucket configuration required for migration")
    
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

def migrate_single_dataset_service(dataset_id: str, config_name: Optional[str], 
                                 revision: Optional[str], config) -> bool:
    """Migrate a single dataset to bucket-specific storage"""
    if not config or not config.s3_bucket_name:
        raise ValueError("S3 bucket configuration required for migration")
    
    # Determine if this is a public dataset by checking both paths
    is_public = False
    public_legacy_path = _get_legacy_dataset_path(dataset_id, config_name, revision, config, is_public=True)
    if public_legacy_path.exists():
        is_public = True
    
    return migrate_dataset_to_bucket_storage(dataset_id, config_name, revision, config, is_public) 