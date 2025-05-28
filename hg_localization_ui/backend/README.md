# HG-Localization UI Backend - Refactored Structure

This document describes the refactored backend structure that breaks down the large `main.py` file into smaller, more manageable modules.

## New Structure

```
backend/
├── main_new.py              # New main application file (clean and minimal)
├── main.py                  # Original main file (kept for reference)
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

1. **Test the new structure**: Run `python main_new.py` to ensure everything works
2. **Update imports**: If you have any external scripts importing from the old main.py, update them to import from the appropriate new modules
3. **Replace main.py**: Once confident, you can replace `main.py` with `main_new.py`

## Running the Refactored Application

```bash
# Run with the new structure
python main_new.py

# Or with uvicorn directly
uvicorn main_new:app --host 0.0.0.0 --port 8000 --reload
```

## Adding New Features

When adding new features:

1. **Models**: Add new Pydantic models to `models.py`
2. **Business Logic**: Add service functions to the appropriate service module
3. **API Endpoints**: Add router endpoints to the appropriate router module
4. **Configuration**: Add configuration helpers to `config.py` if needed

This structure makes the codebase much more maintainable and follows FastAPI best practices for larger applications. 