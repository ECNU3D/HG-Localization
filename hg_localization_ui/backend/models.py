from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class S3Config(BaseModel):
    s3_bucket_name: str = Field(..., description="S3 bucket name")
    s3_endpoint_url: Optional[str] = Field(None, description="S3 endpoint URL for S3-compatible services")
    aws_access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(None, description="AWS secret access key")
    s3_data_prefix: Optional[str] = Field("", description="S3 data prefix")

class ConfigStatus(BaseModel):
    configured: bool
    has_credentials: bool
    credentials_valid: bool = False  # New field to track if credentials actually work
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