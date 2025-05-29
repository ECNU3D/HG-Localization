# HG-Localization UI Backend - Refactored Structure

This document describes the refactored backend structure that breaks down the large `main.py` file into smaller, more manageable modules.

## New Structure

```
backend/
├── main.py              # New main application file (clean and minimal)
├── main_old.py                  # Original main file (kept for reference)
├── models.py                # Pydantic models and data structures
├── config.py                # Configuration management and utilities
├── websocket_manager.py     # WebSocket connection management
├── routers/                 # API route handlers
│   ├── __init__.py
│   ├── config_router.py     # Configuration endpoints
│   ├── dataset_router.py    # Dataset endpoints
│   ├── model_router.py      # Model endpoints
│   └── migration_router.py  # Migration endpoints
├── services/                # Business logic layer
│   ├── __init__.py
│   ├── dataset_service.py   # Dataset business logic
│   ├── model_service.py     # Model business logic
│   └── migration_service.py # Migration business logic
└── requirements.txt
```

## Module Descriptions

### Core Modules

- **`main_new.py`**: The new main application file. Clean and minimal, only handles FastAPI app setup, middleware, router inclusion, and basic endpoints.

- **`models.py`**: Contains all Pydantic models for request/response validation:
  - `S3Config`, `ConfigStatus`
  - `DatasetInfo`, `DatasetDownloadRequest`, `DatasetPreview`
  - `ModelInfo`, `ModelDownloadRequest`, `ModelCard`, `ModelConfig`
  - `CodeExample`, `MigrationStatus`, `MigrationResult`

- **`config.py`**: Configuration management utilities:
  - Cookie encoding/decoding functions
  - Configuration extraction from requests
  - Configuration status helpers

- **`websocket_manager.py`**: WebSocket connection management:
  - `ConnectionManager` class for handling real-time updates
  - Global manager instance

### Routers (API Endpoints)

- **`config_router.py`**: Configuration management endpoints
  - `POST /api/config` - Set S3 configuration
  - `GET /api/config/status` - Get configuration status
  - `DELETE /api/config` - Clear configuration

- **`dataset_router.py`**: Dataset management endpoints
  - `GET /api/datasets/cached` - List cached datasets
  - `GET /api/datasets/s3` - List S3 datasets
  - `GET /api/datasets/all` - List all datasets
  - `POST /api/datasets/cache` - Cache a dataset
  - `GET /api/datasets/{id}/download` - Download dataset as ZIP
  - `GET /api/datasets/{id}/preview` - Preview dataset
  - `GET /api/datasets/{id}/card` - Get dataset card
  - `GET /api/datasets/{id}/examples` - Get code examples

- **`model_router.py`**: Model management endpoints
  - `GET /api/models/cached` - List cached models
  - `GET /api/models/s3` - List S3 models
  - `GET /api/models/all` - List all models
  - `POST /api/models/cache` - Cache a model
  - `GET /api/models/{id}/card` - Get model card
  - `GET /api/models/{id}/config` - Get model config
  - `GET /api/models/{id}/examples` - Get code examples

- **`migration_router.py`**: Migration management endpoints
  - `GET /api/datasets/migration/status` - Check migration status
  - `POST /api/datasets/migration/migrate-all` - Migrate all datasets
  - `POST /api/datasets/migration/{id}/migrate` - Migrate single dataset

- **`model_testing_router.py`**: Model testing endpoints (NEW)
  - `GET /api/model-testing/config` - Get model testing configuration
  - `POST /api/model-testing/check-availability` - Check if model is available
  - `POST /api/model-testing/test` - Send prompt to model and get response

### Services (Business Logic)

- **`dataset_service.py`**: Dataset business logic
  - Dataset listing and filtering
  - ZIP file creation
  - Dataset preview generation
  - Code example generation
  - Background caching tasks

- **`model_service.py`**: Model business logic
  - Model listing and filtering
  - Model card/config retrieval
  - Code example generation
  - Background caching tasks

- **`migration_service.py`**: Migration business logic
  - Migration status checking
  - Dataset migration operations

## Benefits of the Refactored Structure

1. **Separation of Concerns**: Each module has a single responsibility
2. **Maintainability**: Easier to find and modify specific functionality
3. **Testability**: Individual modules can be tested in isolation
4. **Scalability**: Easy to add new features without affecting existing code
5. **Readability**: Much smaller, focused files are easier to understand
6. **Reusability**: Service functions can be reused across different endpoints

## Migration Guide

To switch from the old structure to the new one:

1. **Test the new structure**: Run `python main.py` to ensure everything works
2. **Update imports**: If you have any external scripts importing from the old main.py, update them to import from the appropriate new modules
3. **Replace main.py**: Once confident, you can replace `main.py` with `main.py`

## Running the Refactored Application

```bash
# Run with the new structure
python main.py

# Or with uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Adding New Features

When adding new features:

1. **Models**: Add new Pydantic models to `models.py`
2. **Business Logic**: Add service functions to the appropriate service module
3. **API Endpoints**: Add router endpoints to the appropriate router module
4. **Configuration**: Add configuration helpers to `config.py` if needed

This structure makes the codebase much more maintainable and follows FastAPI best practices for larger applications.

## Configuration

The backend supports configuration through environment variables:

### S3 Configuration (Existing)
- `HGLOC_S3_BUCKET_NAME`: S3 bucket name for storing datasets/models
- `HGLOC_S3_ENDPOINT_URL`: S3 endpoint URL (optional, defaults to AWS)
- `HGLOC_S3_DATA_PREFIX`: S3 data prefix (optional)

### Model Testing Configuration (New)
- `HGLOC_ENABLE_MODEL_TESTING`: Set to 'true' to enable model testing functionality (default: false)
- `HGLOC_OPENAI_BASE_URL`: OpenAI compatible API base URL including API version (e.g., http://localhost:8001/v1)
- `HGLOC_MODEL_TESTING_TIMEOUT`: Request timeout in seconds (default: 30)

### Example Configuration
Create a `.env` file in the backend directory:

```bash
# Basic S3 configuration
HGLOC_S3_BUCKET_NAME=your-bucket-name
HGLOC_S3_ENDPOINT_URL=https://s3.amazonaws.com
HGLOC_S3_DATA_PREFIX=hg-localization

# Enable model testing with local vLLM server
HGLOC_ENABLE_MODEL_TESTING=true
HGLOC_OPENAI_BASE_URL=http://localhost:8001/v1
HGLOC_MODEL_TESTING_TIMEOUT=30
```

### Model Testing Feature
The model testing feature allows users to:
1. Check if a specific model is available and functional at an OpenAI compatible endpoint
2. Send prompts to the model and receive responses
3. Test models before deploying them in production
4. **Upload images for vision models (PNG/JPEG support)**

This feature is disabled by default and requires explicit configuration.

#### Model Testing Endpoints
- `GET /api/model-testing/config` - Get current model testing configuration and status
- `POST /api/model-testing/check-availability` - Test if a specific model is functional by sending a test prompt
- `POST /api/model-testing/test` - Send a prompt (with optional image) to the model and get a response

#### Implementation Details
The model testing functionality includes:

**Feature Flag Control**: The entire functionality can be enabled/disabled via `HGLOC_ENABLE_MODEL_TESTING` environment variable.

**Model Availability Check**: 
- Sends a minimal test prompt ("Hi") to the model using `/chat/completions` endpoint
- Returns detailed status including availability and error messages  
- Handles timeouts and connection errors gracefully
- More reliable than listing endpoints since it tests actual model functionality

**Model Testing**:
- Sends prompts using the OpenAI-compatible `/chat/completions` endpoint
- **Supports image uploads for vision models** with PNG and JPEG formats
- Images are base64-encoded and sent in OpenAI Vision API format
- Supports configurable parameters (max_tokens, temperature)
- Returns both success responses and detailed error information
- Users can specify custom model names that match their server configuration

**Image Upload Support**:
- Drag-and-drop interface for easy image uploads
- File type validation (PNG/JPEG only)
- File size validation (up to 10MB)
- Real-time image preview
- Base64 encoding for API transmission
- Compatible with OpenAI Vision API format

**Base URL Configuration**:
- Users specify the complete API base URL including version (e.g., `/v1`)
- No hardcoded paths - supports different OpenAI-compatible server configurations
- Works with vLLM, text-generation-inference, OpenAI API, and other compatible servers

**Error Handling**:
- Graceful handling of network timeouts and connection failures
- Proper error messages for unavailable models or invalid API keys
- Feature-disabled responses when functionality is turned off
- Configuration validation (ensures base URL is set when feature is enabled)
- Improved error parsing for common issues (model not found, unauthorized access)

#### Security Considerations
- API keys are passed through but not stored on the server
- All requests use HTTPS when properly configured
- Feature can be completely disabled in production environments
- No model responses are logged or cached

#### Smart Response Formatting
The frontend includes intelligent response formatting that automatically detects and renders different content types:

**Supported Content Types**:
- **JSON**: Automatically formats and syntax highlights JSON responses
- **Markdown**: Renders markdown with headers, lists, code blocks, links, etc.
- **Code**: Formats code with syntax highlighting for various programming languages
- **Plain Text**: Displays plain text with proper line breaks and formatting

**Detection Examples**:
- JSON: `{"message": "Hello", "status": "success"}`
- Markdown: Content with `# Headers`, `**bold**`, `*italic*`, `` `code` ``, etc.
- Code: Content with keywords like `function`, `def`, `class`, comments, etc.
- Plain Text: Everything else is displayed as formatted text

The formatter preserves the copy functionality, allowing users to copy the raw response content regardless of how it's displayed. 