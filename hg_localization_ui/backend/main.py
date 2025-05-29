import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

# Add the parent directory to sys.path to import hg_localization
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Import routers
from routers.config_router import router as config_router
from routers.dataset_router import router as dataset_router
from routers.model_router import router as model_router
from routers.migration_router import router as migration_router
from routers.model_testing_router import router as model_testing_router

# Import WebSocket manager
from websocket_manager import manager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting HG-Localization UI Backend...")
    yield
    # Shutdown
    print("Shutting down HG-Localization UI Backend...")

app = FastAPI(
    title="HG-Localization UI API",
    description="API for managing Hugging Face datasets with S3 integration",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(config_router)
app.include_router(dataset_router)
app.include_router(model_router)
app.include_router(migration_router)
app.include_router(model_testing_router)

# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Echo back for now, can be extended for specific commands
            await manager.send_personal_message(f"Echo: {data}", websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Health check
@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "service": "hg-localization-ui"}

# Serve static files (for production)
if Path("static").exists():
    app.mount("/", StaticFiles(directory="static", html=True), name="static")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 