import json
import base64
import os
from typing import Optional
from fastapi import Request

from models import S3Config, ConfigStatus, DefaultConfig, AppConfig
from hg_localization import HGLocalizationConfig
from hg_localization.s3_utils import _get_s3_client

# Cookie configuration
COOKIE_NAME = "hg_localization_config"
COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 days in seconds

def get_default_config() -> DefaultConfig:
    """Load default configuration values from environment variables"""
    return DefaultConfig(
        s3_bucket_name=os.getenv("HGLOC_S3_BUCKET_NAME"),
        s3_endpoint_url=os.getenv("HGLOC_S3_ENDPOINT_URL"),
        s3_data_prefix=os.getenv("HGLOC_S3_DATA_PREFIX")
    )

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
    """Determine if this is public access only (no credentials provided or credentials invalid)"""
    if config is None or not config.has_credentials():
        return True
    
    # Test if credentials are actually valid
    try:
        s3_client = _get_s3_client(config)
        return s3_client is None
    except Exception:
        return True

def get_config_status_from_config(config: Optional[HGLocalizationConfig]) -> ConfigStatus:
    """Get configuration status from HGLocalizationConfig object"""
    if not config:
        return ConfigStatus(
            configured=False,
            has_credentials=False,
            credentials_valid=False,
            bucket_name=None,
            endpoint_url=None,
            data_prefix=None
        )
    
    # Check if credentials are provided
    has_credentials = config.has_credentials()
    credentials_valid = False
    
    # If credentials are provided, test if they actually work
    if has_credentials:
        try:
            s3_client = _get_s3_client(config)
            credentials_valid = s3_client is not None
        except Exception as e:
            print(f"Error validating S3 credentials: {e}")
            credentials_valid = False
    
    return ConfigStatus(
        configured=bool(config.s3_bucket_name),
        has_credentials=has_credentials,
        credentials_valid=credentials_valid,
        bucket_name=config.s3_bucket_name,
        endpoint_url=config.s3_endpoint_url,
        data_prefix=config.s3_data_prefix
    )

def get_app_config() -> AppConfig:
    """Load application configuration from environment variables"""
    return AppConfig(
        openai_base_url=os.getenv("HGLOC_OPENAI_BASE_URL"),
        enable_model_testing=os.getenv("HGLOC_ENABLE_MODEL_TESTING", "false").lower() == "true",
        model_testing_timeout=int(os.getenv("HGLOC_MODEL_TESTING_TIMEOUT", "30"))
    )