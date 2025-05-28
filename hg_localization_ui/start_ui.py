#!/usr/bin/env python3
"""
Main startup script for HG-Localization UI
Supports starting backend, frontend, or both services
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

def start_backend():
    """Start the FastAPI backend server"""
    print(">> Starting HG-Localization UI Backend...")
    
    # Add the parent directory to sys.path to import hg_localization
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    # Change to backend directory
    backend_dir = Path(__file__).parent / "backend"
    original_cwd = os.getcwd()
    
    try:
        os.chdir(backend_dir)
        
        # Add backend directory to Python path
        sys.path.insert(0, str(backend_dir))
        
        import uvicorn
        from main import app
        
        print("OK Backend will be available at: http://localhost:8000")
        print(">> API documentation at: http://localhost:8000/docs")
        
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
    except ImportError as e:
        print(f"ERROR: Error importing backend dependencies: {e}")
        print("TIP: Make sure you've installed the backend requirements:")
        print("   cd backend && pip install -r requirements.txt")
        os.chdir(original_cwd)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Error starting backend: {e}")
        os.chdir(original_cwd)
        sys.exit(1)

def start_frontend():
    """Start the React frontend development server"""
    print(">> Starting HG-Localization UI Frontend...")
    
    frontend_dir = Path(__file__).parent / "frontend"
    
    if not frontend_dir.exists():
        print(f"ERROR: Frontend directory not found at {frontend_dir}")
        sys.exit(1)
    
    # Check if node_modules exists
    node_modules = frontend_dir / "node_modules"
    if not node_modules.exists():
        print(">> Installing frontend dependencies...")
        try:
            subprocess.run(["npm", "install"], cwd=frontend_dir, check=True, shell=True)
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Error installing dependencies: {e}")
            sys.exit(1)
    
    print("OK Frontend will be available at: http://localhost:3000")
    
    # Start the development server
    try:
        subprocess.run(["npm", "start"], cwd=frontend_dir, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Error starting frontend: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nSTOP: Frontend server stopped.")

def start_both():
    """Start both backend and frontend in separate processes"""
    print(">> Starting HG-Localization UI (Backend + Frontend)...")
    
    # Start backend in a separate process
    backend_process = subprocess.Popen(
        [sys.executable, str(Path(__file__)), "--backend"]
        # Note: Not capturing stdout/stderr so logs are visible
    )
    
    # Give backend time to start
    print(">> Waiting for backend to start...")
    time.sleep(5)
    
    # Check if backend is running
    try:
        import requests
        response = requests.get("http://localhost:8000/api/health", timeout=5)
        if response.status_code == 200:
            print("OK Backend started successfully")
        else:
            print("WARN: Backend may not be ready yet")
    except:
        print("WARN: Backend may not be ready yet")
    
    # Start frontend in main thread
    try:
        start_frontend()
    except KeyboardInterrupt:
        print("\nSTOP: Shutting down services...")
        backend_process.terminate()
        backend_process.wait(timeout=5)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Start HG-Localization UI services",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start_ui.py              # Start both backend and frontend
  python start_ui.py --backend    # Start only backend
  python start_ui.py --frontend   # Start only frontend
        """
    )
    
    parser.add_argument(
        "--backend", 
        action="store_true", 
        help="Start only the backend server"
    )
    parser.add_argument(
        "--frontend", 
        action="store_true", 
        help="Start only the frontend server"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(">> HG-Localization UI Startup Script")
    print("=" * 60)
    
    if args.backend and args.frontend:
        print("ERROR: Cannot specify both --backend and --frontend")
        sys.exit(1)
    elif args.backend:
        start_backend()
    elif args.frontend:
        start_frontend()
    else:
        # Default: start both
        start_both()

if __name__ == "__main__":
    main() 