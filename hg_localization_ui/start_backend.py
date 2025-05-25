#!/usr/bin/env python3
"""
Startup script for HG-Localization UI Backend
"""
import os
import sys
import uvicorn
from pathlib import Path

# Add the parent directory to sys.path to import hg_localization
sys.path.insert(0, str(Path(__file__).parent.parent))

def main():
    """Start the FastAPI backend server"""
    print("Starting HG-Localization UI Backend...")
    print("Backend will be available at: http://localhost:8000")
    print("API documentation at: http://localhost:8000/docs")
    print("Press Ctrl+C to stop the server")
    
    # Change to backend directory
    backend_dir = Path(__file__).parent / "backend"
    original_cwd = os.getcwd()
    
    try:
        os.chdir(backend_dir)
        
        # Add backend directory to Python path
        sys.path.insert(0, str(backend_dir))
        
        # Start the server
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
    finally:
        os.chdir(original_cwd)

if __name__ == "__main__":
    main() 