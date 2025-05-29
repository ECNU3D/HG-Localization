from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, ConfigDict

class S3Config(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    s3_bucket_name: str = Field(..., description="S3 bucket name")
    s3_endpoint_url: Optional[str] = Field(None, description="S3 endpoint URL for S3-compatible services")
    aws_access_key_id: Optional[str] = Field(None, description="AWS access key ID")
    aws_secret_access_key: Optional[str] = Field(None, description="AWS secret access key")
    s3_data_prefix: Optional[str] = Field("", description="S3 data prefix")

class DefaultConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    s3_bucket_name: Optional[str] = Field(None, description="Default S3 bucket name from environment")
    s3_endpoint_url: Optional[str] = Field(None, description="Default S3 endpoint URL from environment")
    s3_data_prefix: Optional[str] = Field(None, description="Default S3 data prefix from environment")

class ConfigStatus(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    configured: bool
    has_credentials: bool
    credentials_valid: bool = False  # New field to track if credentials actually work
    bucket_name: Optional[str]
    endpoint_url: Optional[str]
    data_prefix: Optional[str]

class DatasetInfo(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
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
    model_config = ConfigDict(protected_namespaces=())
    
    dataset_id: str
    config_name: Optional[str] = None
    revision: Optional[str] = None
    trust_remote_code: bool = False
    make_public: bool = False

class DatasetPreview(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    dataset_id: str
    config_name: Optional[str]
    revision: Optional[str]
    features: Dict[str, Any]
    num_rows: Dict[str, int]
    sample_data: List[Dict[str, Any]]

class ModelInfo(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
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
    model_config = ConfigDict(protected_namespaces=())
    
    model_id: str
    revision: Optional[str] = None
    make_public: bool = False
    metadata_only: bool = True

class ModelCard(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    content: str

class ModelConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    config: Dict[str, Any]

class CodeExample(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    title: str
    description: str
    code: str
    language: str = "python"

class MigrationStatus(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    needs_migration: bool
    legacy_datasets_count: int
    current_bucket: Optional[str]
    migration_available: bool

class MigrationResult(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    success: bool
    migrated_count: int
    failed_count: int
    message: str

# New models for OpenAI integration and model testing
class OpenAIConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    base_url: str = Field(..., description="OpenAI compatible API base URL")
    api_key: Optional[str] = Field(None, description="API key for OpenAI compatible service")
    timeout: int = Field(30, description="Request timeout in seconds")

class ModelTestRequest(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    model_id: str = Field(..., description="Model ID to test")
    api_key: str = Field(..., description="API key for the request")
    message: str = Field(..., description="Test message to send to the model")
    image_data: Optional[str] = Field(None, description="Base64 encoded image data")
    image_type: Optional[str] = Field(None, description="Image MIME type (image/png or image/jpeg)")
    image_filename: Optional[str] = Field(None, description="Original filename of the uploaded image")

class ModelTestResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    success: bool
    response: Optional[str] = None
    error: Optional[str] = None

class ModelAvailabilityCheck(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    
    model_id: str
    available: bool
    error: Optional[str] = None

class AppConfig(BaseModel):
    """Application-wide configuration settings"""
    model_config = ConfigDict(protected_namespaces=())
    
    openai_base_url: Optional[str] = Field(None, description="OpenAI compatible API base URL")
    enable_model_testing: bool = Field(False, description="Enable model testing functionality")
    model_testing_timeout: int = Field(30, description="Timeout for model testing requests") 