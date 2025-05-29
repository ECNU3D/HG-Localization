# Default Configuration Implementation

This document describes the implementation of default configuration values for the HG-Localization UI, allowing bucket name and endpoint URL to be pre-populated from environment variables.

## Overview

The system now supports loading default configuration values from environment variables, which are automatically populated in the configuration form when the application starts. This makes it easier to deploy the application with pre-configured settings.

## Environment Variables

The following environment variables are supported for default configuration:

- `HGLOC_S3_BUCKET_NAME`: Default S3 bucket name
- `HGLOC_S3_ENDPOINT_URL`: Default S3 endpoint URL (for S3-compatible services)
- `HGLOC_S3_DATA_PREFIX`: Default S3 data prefix

## Backend Changes

### 1. Models (`models.py`)

Added a new `DefaultConfig` model:

```python
class DefaultConfig(BaseModel):
    s3_bucket_name: Optional[str] = Field(None, description="Default S3 bucket name from environment")
    s3_endpoint_url: Optional[str] = Field(None, description="Default S3 endpoint URL from environment")
    s3_data_prefix: Optional[str] = Field(None, description="Default S3 data prefix from environment")
```

### 2. Configuration (`config.py`)

Added a function to load default values from environment variables:

```python
def get_default_config() -> DefaultConfig:
    """Load default configuration values from environment variables"""
    return DefaultConfig(
        s3_bucket_name=os.getenv("HGLOC_S3_BUCKET_NAME"),
        s3_endpoint_url=os.getenv("HGLOC_S3_ENDPOINT_URL"),
        s3_data_prefix=os.getenv("HGLOC_S3_DATA_PREFIX")
    )
```

### 3. API Router (`routers/config_router.py`)

Added a new endpoint to retrieve default configuration values:

```python
@router.get("/defaults", response_model=DefaultConfig)
async def get_default_configuration():
    """Get default configuration values from environment variables"""
    return get_default_config()
```

## Frontend Changes

### 1. Types (`types/index.ts`)

Added the `DefaultConfig` interface:

```typescript
export interface DefaultConfig {
  s3_bucket_name?: string;
  s3_endpoint_url?: string;
  s3_data_prefix?: string;
}
```

### 2. API Client (`api/client.ts`)

Added the new endpoint to the API client:

```typescript
getDefaults: (): Promise<AxiosResponse<DefaultConfig>> =>
  apiClient.get('/config/defaults'),
```

### 3. Hooks (`hooks/useConfig.ts`)

Added a new hook to fetch default configuration:

```typescript
export const useDefaultConfig = () => {
  return useQuery({
    queryKey: ['config', 'defaults'],
    queryFn: async () => {
      const response = await api.config.getDefaults();
      return response.data;
    },
    staleTime: 1000 * 60 * 60, // 1 hour - defaults don't change often
  });
};
```

### 4. Configuration Page (`pages/ConfigurationPage.tsx`)

Enhanced the configuration page with:

- Automatic loading of default values when the page loads
- Visual indicators showing which fields have default values
- Information banner when default values are available
- Automatic population of form fields with default values

## Usage

### Setting Environment Variables

#### Option 1: Using .env file

Create or update the `.env` file in the `hg_localization_ui` directory:

```bash
# Default S3 Configuration
HGLOC_S3_BUCKET_NAME=my-default-bucket
HGLOC_S3_ENDPOINT_URL=https://s3.amazonaws.com
HGLOC_S3_DATA_PREFIX=datasets/
```

#### Option 2: System Environment Variables

Set the environment variables in your system:

```bash
export HGLOC_S3_BUCKET_NAME=my-default-bucket
export HGLOC_S3_ENDPOINT_URL=https://s3.amazonaws.com
export HGLOC_S3_DATA_PREFIX=datasets/
```

#### Option 3: Docker Environment

When using Docker, pass the environment variables:

```bash
docker run -e HGLOC_S3_BUCKET_NAME=my-default-bucket \
           -e HGLOC_S3_ENDPOINT_URL=https://s3.amazonaws.com \
           -e HGLOC_S3_DATA_PREFIX=datasets/ \
           hg-localization-ui
```

### User Experience

1. **First Time Setup**: When users visit the configuration page for the first time, any fields with default values will be:
   - Pre-populated with the default values
   - Highlighted with a blue border and background
   - Labeled with a "Default from environment" badge

2. **Information Banner**: If any default values are available, an information banner appears at the top of the configuration page explaining that some values have been pre-filled.

3. **Customization**: Users can modify any pre-filled values as needed. The visual indicators help them understand which values came from environment variables.

## API Endpoints

### GET `/api/config/defaults`

Returns the default configuration values loaded from environment variables.

**Response:**
```json
{
  "s3_bucket_name": "my-default-bucket",
  "s3_endpoint_url": "https://s3.amazonaws.com",
  "s3_data_prefix": "datasets/"
}
```

**Response when no defaults are set:**
```json
{
  "s3_bucket_name": null,
  "s3_endpoint_url": null,
  "s3_data_prefix": null
}
```

## Benefits

1. **Easier Deployment**: Administrators can pre-configure the application with organization-specific settings
2. **Reduced User Friction**: Users don't need to manually enter common configuration values
3. **Consistency**: Ensures consistent configuration across deployments
4. **Flexibility**: Users can still override default values when needed
5. **Visual Clarity**: Clear indicators show which values are defaults vs. user-configured

## Testing

The implementation has been tested to ensure:

- Default values are correctly loaded from environment variables
- The API endpoint returns the expected response format
- Frontend correctly fetches and displays default values
- Visual indicators work properly
- Form submission works with both default and custom values

## Backward Compatibility

This implementation is fully backward compatible:

- Existing configurations continue to work unchanged
- The feature is optional - if no environment variables are set, the behavior is identical to before
- No breaking changes to existing API endpoints or data structures 