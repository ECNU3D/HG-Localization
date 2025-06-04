# HG-Localization UI

A modern web interface for managing Hugging Face datasets with S3 integration, built on top of the `hg_localization` library.

## Features

- **Dataset Discovery**: Browse both public and private datasets with rich metadata
- **Dataset Preview**: View sample records and schema information
- **Model Card Integration**: Display dataset documentation and usage examples
- **S3 Configuration**: Dynamic S3 endpoint and credential management
- **Download Management**: Track dataset downloads and local cache status
- **Code Examples**: Auto-generated Python code snippets for dataset usage
- **Responsive Design**: Modern, mobile-friendly interface

## Architecture

### Backend (FastAPI)
- RESTful API built with FastAPI
- Integration with `hg_localization` library
- Dynamic S3 configuration management
- Real-time dataset operations
- WebSocket support for download progress

### Frontend (Next.js + TypeScript)
- Modern Next.js 15 with TypeScript
- File-based routing with dynamic routes
- Tailwind CSS for styling
- React Query for state management
- Monaco Editor for code display
- Server-side rendering capabilities

## Quick Start

### Prerequisites
- Python 3.8+
- Node.js 16+
- npm or yarn
- Git

### Option 1: Using the Startup Script (Recommended)

1. **Clone and setup the repository:**
```bash
git clone https://github.com/ECNU3D/hg-localization.git
cd hg-localization
```

2. **Install Python dependencies:**
```bash
cd hg_localization_ui/backend
pip install -r requirements.txt
cd ..
```

3. **Install Node.js dependencies:**
```bash
cd frontend
npm install
cd ..
```

4. **Start both services:**
```bash
# Development mode (Next.js dev server)
python start_ui.py

# Production mode (Next.js optimized build)
python start_production.py

# Custom ports (development)
python start_custom_ports.py --backend-port 8001 --frontend-port 3001

# With visible logs (see all hg_localization library logs)
python start_with_logs.py

# Custom ports with logs
python start_with_logs_custom_ports.py -b 8001 -f 3001

# Simple startup (if threading issues occur)
python start_simple.py

# Or start individual services:
python start_ui.py --backend    # Backend only
python start_ui.py --frontend   # Frontend only (Next.js dev)
python start_production.py --frontend   # Frontend only (Next.js production)
```

5. **Access the application:**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:8000
   - API Documentation: http://localhost:8000/docs

### Option 2: Manual Setup

#### Backend Setup

1. Install Python dependencies:
```bash
cd backend
pip install -r requirements.txt
```

2. Start the FastAPI server:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

#### Frontend Setup

1. Install Node.js dependencies:
```bash
cd frontend
npm install
```

2. Start the development server:
```bash
npm run dev
```

3. Open http://localhost:3000 in your browser

### Option 3: Docker Deployment

1. **Using Docker Compose (Recommended):**
```bash
# Copy environment template
cp env.example .env
# Edit .env with your S3 configuration

# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

2. **Manual Docker Build:**
```bash
# Build backend
docker build -f Dockerfile.backend -t hg-localization-backend ..

# Build frontend
docker build -f Dockerfile.frontend -t hg-localization-frontend .

# Run backend
docker run -p 8000:8000 hg-localization-backend

# Run frontend
docker run -p 3000:80 hg-localization-frontend
```

## Configuration

### S3 Settings
The UI allows dynamic configuration of S3 settings:

- **S3 Bucket Name**: Required for all operations
- **S3 Endpoint URL**: Optional, for S3-compatible services
- **AWS Access Key ID**: Required for private dataset access
- **AWS Secret Access Key**: Required for private dataset access
- **S3 Data Prefix**: Optional namespace within the bucket

### Access Modes

1. **Public Access**: Only bucket name required
   - View public datasets only
   - Download public datasets
   - Read-only access

2. **Private Access**: Full credentials required
   - View all datasets (public + private)
   - Upload new datasets
   - Make datasets public
   - Full read/write access

## API Endpoints

### Configuration
- `POST /api/config` - Set S3 configuration
- `GET /api/config/status` - Get current configuration status

### Datasets
- `GET /api/datasets/local` - List local datasets
- `GET /api/datasets/s3` - List S3 datasets
- `GET /api/datasets/{dataset_id}` - Get dataset details
- `POST /api/datasets/download` - Download dataset
- `POST /api/datasets/upload` - Upload dataset
- `GET /api/datasets/{dataset_id}/preview` - Preview dataset records

### Dataset Cards
- `GET /api/datasets/{dataset_id}/card` - Get dataset card content
- `GET /api/datasets/{dataset_id}/examples` - Get usage examples

## Development

### Backend Development
```bash
cd backend
pip install -r requirements-dev.txt
pytest  # Run tests
black .  # Format code
```

### Frontend Development
```bash
cd frontend

# Development server (hot reload)
npm run dev

# Production build
npm run build

# Start production server (after build)
npm run start

# Linting
npm run lint

# Run tests  
npm run test
```

## Deployment

### Docker Deployment
```bash
docker-compose up -d
```

### Manual Deployment
1. Build frontend: `cd frontend && npm run build`
2. Copy build files to backend static directory
3. Deploy FastAPI with production ASGI server

## Troubleshooting

### Common Issues

#### "signal only works in main thread" Error
If you encounter this error when starting both services together:
```bash
# Use the alternative startup script
python start_simple.py

# Or start services individually
python start_ui.py --backend    # In one terminal
python start_ui.py --frontend   # In another terminal
```

#### Frontend Not Starting on Windows
If npm commands fail:
- Ensure Node.js and npm are properly installed
- Try running commands in Command Prompt instead of Git Bash
- Check that npm is in your PATH

#### Backend Import Errors
If the backend can't find modules:
```bash
# Ensure you're in the correct directory
cd hg_localization_ui/backend
pip install -r requirements.txt
```

#### Port Already in Use
If ports 3000 or 8000 are already in use:
- Stop other services using those ports
- Or modify the port numbers in the startup scripts

### Getting Help
- Check the logs in the terminal output
- Verify setup with: `python hg_localization_ui/test_setup.py`
- Test startup with: `python hg_localization_ui/test_startup.py`

## License

MIT License - see LICENSE file for details. 