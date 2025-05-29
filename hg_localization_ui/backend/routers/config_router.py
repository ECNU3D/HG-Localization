from fastapi import APIRouter, Request, Response
from models import S3Config, ConfigStatus, DefaultConfig
from config import (
    encode_config_cookie, 
    get_config_from_request, 
    get_config_status_from_config,
    get_default_config,
    COOKIE_NAME,
    COOKIE_MAX_AGE
)
from hg_localization import HGLocalizationConfig

router = APIRouter(prefix="/api/config", tags=["configuration"])

@router.post("", response_model=ConfigStatus)
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

@router.get("/status", response_model=ConfigStatus)
async def get_config_status(request: Request):
    """Get current configuration status from cookie"""
    config = get_config_from_request(request)
    return get_config_status_from_config(config)

@router.delete("")
async def clear_config(response: Response):
    """Clear S3 configuration cookie"""
    response.delete_cookie(
        key=COOKIE_NAME,
        httponly=True,
        secure=False,  # Set to True in production with HTTPS
        samesite="lax"
    )
    return {"message": "Configuration cleared successfully"}

@router.get("/defaults", response_model=DefaultConfig)
async def get_default_configuration():
    """Get default configuration values from environment variables"""
    return get_default_config() 