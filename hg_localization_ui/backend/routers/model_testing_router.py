from typing import Optional
from fastapi import APIRouter, HTTPException, Request
import httpx
import asyncio
import json

from models import ModelTestRequest, ModelTestResponse, ModelAvailabilityCheck, AppConfig
from config import get_app_config

router = APIRouter(prefix="/api/model-testing", tags=["model-testing"])

@router.get("/config")
async def get_model_testing_config():
    """Get model testing configuration"""
    app_config = get_app_config()
    
    if not app_config.enable_model_testing:
        raise HTTPException(status_code=404, detail="Model testing is disabled")
    
    return {
        "enabled": app_config.enable_model_testing,
        "base_url": app_config.openai_base_url,
        "timeout": app_config.model_testing_timeout
    }

@router.post("/check-availability", response_model=ModelAvailabilityCheck)
async def check_model_availability(model_id: str, api_key: str):
    """Check if a model is available by sending a small test prompt"""
    app_config = get_app_config()
    
    if not app_config.enable_model_testing:
        raise HTTPException(status_code=404, detail="Model testing is disabled")
    
    if not app_config.openai_base_url:
        raise HTTPException(status_code=500, detail="OpenAI base URL not configured")
    
    try:
        # Test model availability by sending a simple prompt
        async with httpx.AsyncClient(timeout=app_config.model_testing_timeout) as client:
            # Remove hardcoded /v1 - let users specify the full path in their base URL
            chat_url = f"{app_config.openai_base_url.rstrip('/')}/chat/completions"
            
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            # Send a minimal test prompt to check if the model is working
            payload = {
                "model": model_id,
                "messages": [
                    {
                        "role": "user", 
                        "content": "Hi"
                    }
                ],
                "max_tokens": 5,  # Minimal response to save resources
                "temperature": 0.1
            }
            
            response = await client.post(chat_url, headers=headers, json=payload)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check if we got a valid response structure
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    return ModelAvailabilityCheck(
                        model_id=model_id,
                        available=True,
                        error=None
                    )
                else:
                    return ModelAvailabilityCheck(
                        model_id=model_id,
                        available=False,
                        error="Model responded but returned invalid response format"
                    )
            else:
                error_detail = f"Model test failed with status {response.status_code}"
                try:
                    error_data = response.json()
                    if "error" in error_data:
                        error_detail = error_data["error"].get("message", error_detail)
                        # Common error patterns to provide better user feedback
                        if "model" in error_detail.lower() and "not found" in error_detail.lower():
                            error_detail = f"Model '{model_id}' not found on server"
                        elif "unauthorized" in error_detail.lower():
                            error_detail = "Invalid API key or unauthorized access"
                except:
                    pass
                
                return ModelAvailabilityCheck(
                    model_id=model_id,
                    available=False,
                    error=error_detail
                )
                
    except httpx.TimeoutException:
        return ModelAvailabilityCheck(
            model_id=model_id,
            available=False,
            error="Request timeout - the model server may be unavailable"
        )
    except httpx.ConnectError:
        return ModelAvailabilityCheck(
            model_id=model_id,
            available=False,
            error="Connection failed - check if the server URL is correct"
        )
    except Exception as e:
        return ModelAvailabilityCheck(
            model_id=model_id,
            available=False,
            error=f"Error checking model availability: {str(e)}"
        )

@router.post("/test", response_model=ModelTestResponse)
async def test_model(request: ModelTestRequest):
    """Test a model by sending a prompt to the OpenAI compatible endpoint"""
    app_config = get_app_config()
    
    if not app_config.enable_model_testing:
        raise HTTPException(status_code=404, detail="Model testing is disabled")
    
    if not app_config.openai_base_url:
        raise HTTPException(status_code=500, detail="OpenAI base URL not configured")
    
    try:
        async with httpx.AsyncClient(timeout=app_config.model_testing_timeout) as client:
            # Remove hardcoded /v1 - let users specify the full path in their base URL
            chat_url = f"{app_config.openai_base_url.rstrip('/')}/chat/completions"
            
            headers = {
                "Authorization": f"Bearer {request.api_key}",
                "Content-Type": "application/json"
            }
            
            # Build message content - support both text-only and vision models
            if request.image_data and request.image_type:
                # For vision models, use the OpenAI vision format
                message_content = [
                    {
                        "type": "text",
                        "text": request.message
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:{request.image_type};base64,{request.image_data}"
                        }
                    }
                ]
                print(f"message_content: {message_content}")
            else:
                # For text-only models, use simple string content
                message_content = request.message
            
            payload = {
                "model": request.model_id,
                "messages": [
                    {
                        "role": "user", 
                        "content": message_content
                    }
                ],
                "max_tokens": 1024,
                "temperature": 0.7
            }
            
            response = await client.post(chat_url, headers=headers, json=payload)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Extract the response from the OpenAI format
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    content = response_data["choices"][0]["message"]["content"]
                    return ModelTestResponse(
                        success=True,
                        response=content,
                        error=None
                    )
                else:
                    return ModelTestResponse(
                        success=False,
                        response=None,
                        error="No response generated"
                    )
            else:
                error_detail = f"API request failed with status {response.status_code}"
                try:
                    error_data = response.json()
                    if "error" in error_data:
                        error_detail = error_data["error"].get("message", error_detail)
                except:
                    pass
                
                return ModelTestResponse(
                    success=False,
                    response=None,
                    error=error_detail
                )
                
    except httpx.TimeoutException:
        return ModelTestResponse(
            success=False,
            response=None,
            error="Request timeout - the model server may be overloaded"
        )
    except httpx.ConnectError:
        return ModelTestResponse(
            success=False,
            response=None,
            error="Connection failed - check if the server URL is correct"
        )
    except Exception as e:
        return ModelTestResponse(
            success=False,
            response=None,
            error=f"Error testing model: {str(e)}"
        ) 