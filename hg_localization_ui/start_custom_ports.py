#!/usr/bin/env python3
"""
Simple startup script for HG-Localization UI with custom ports
Uses the existing start_ui.py infrastructure with port customization
"""
import subprocess
import sys
import time
import signal
import argparse
import os
from pathlib import Path

def start_services_with_custom_ports(backend_port=8000, frontend_port=3000):
    """Start both backend and frontend services with custom ports"""
    print("=" * 60)
    print(">> HG-Localization UI Startup (Custom Ports)")
    print("=" * 60)
    print(f">> Backend Port: {backend_port}")
    print(f">> Frontend Port: {frontend_port}")
    
    # Set environment variables for the services
    env = os.environ.copy()
    env['HGLOC_BACKEND_PORT'] = str(backend_port)
    env['HGLOC_FRONTEND_PORT'] = str(frontend_port)
    env['PORT'] = str(frontend_port)  # For Next.js dev server
    env['NEXT_PUBLIC_API_URL'] = f'http://localhost:{backend_port}/api'  # Next.js public env var
    
    # Start backend with custom port
    print(">> Starting backend...")
    backend_process = subprocess.Popen([
        sys.executable, "-c", f"""
import sys
import os
from pathlib import Path

# Add the parent directory to sys.path to import hg_localization
sys.path.insert(0, str(Path(__file__).parent.parent))

# Change to backend directory  
backend_dir = Path(__file__).parent / "hg_localization_ui" / "backend"
os.chdir(backend_dir)

# Add backend directory to Python path
sys.path.insert(0, str(backend_dir))

import uvicorn
from main import app

# Update CORS middleware for custom frontend port
from fastapi.middleware.cors import CORSMiddleware
# Clear existing middleware
for i, middleware in enumerate(app.user_middleware):
    if isinstance(middleware, dict) and 'cls' in middleware:
        if issubclass(middleware['cls'], CORSMiddleware):
            app.user_middleware.pop(i)
            break

# Add new CORS middleware with custom ports
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:{frontend_port}", 
        "http://127.0.0.1:{frontend_port}",
        "http://localhost:3000",  # Keep default for compatibility
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print(f"Backend starting on port {backend_port}")
uvicorn.run(
    "main:app",
    host="0.0.0.0",
    port={backend_port},
    reload=True,
    log_level="info"
)
"""], env=env)
    
    # Give backend time to start
    print(">> Waiting for backend to start...")
    time.sleep(3)
    
    # Start frontend with custom port
    print(">> Starting frontend...")
    frontend_dir = Path(__file__).parent / "frontend"
    
    frontend_process = subprocess.Popen([
        "npm", "run", "dev"
    ], cwd=frontend_dir, env=env, shell=True)
    
    print("OK Services starting...")
    print(f">> Backend: http://localhost:{backend_port}")
    print(f">> Frontend: http://localhost:{frontend_port}")
    print(f">> API Docs: http://localhost:{backend_port}/docs")
    print(">> Press Ctrl+C to stop all services")
    
    def signal_handler(sig, frame):
        print("\nSTOP: Stopping services...")
        backend_process.terminate()
        frontend_process.terminate()
        
        # Wait for processes to stop
        try:
            backend_process.wait(timeout=5)
            frontend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            backend_process.kill()
            frontend_process.kill()
        
        print("OK All services stopped")
        sys.exit(0)
    
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Wait for processes
        while True:
            if backend_process.poll() is not None:
                print("ERROR: Backend process stopped unexpectedly")
                break
            if frontend_process.poll() is not None:
                print("ERROR: Frontend process stopped unexpectedly")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

def main():
    parser = argparse.ArgumentParser(
        description="Start HG-Localization UI services with custom ports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start_custom_ports.py                              # Default ports (8000, 3000)
  python start_custom_ports.py --backend-port 8001          # Backend on 8001, frontend on 3000
  python start_custom_ports.py --frontend-port 3001         # Backend on 8000, frontend on 3001
  python start_custom_ports.py -b 9000 -f 4000              # Both custom ports
        """
    )
    
    parser.add_argument(
        "--backend-port", "-b",
        type=int,
        default=8000,
        help="Port for the backend server (default: 8000)"
    )
    
    parser.add_argument(
        "--frontend-port", "-f", 
        type=int,
        default=3000,
        help="Port for the frontend server (default: 3000)"
    )
    
    args = parser.parse_args()
    
    # Validate ports
    if args.backend_port == args.frontend_port:
        print("ERROR: Backend and frontend cannot use the same port")
        sys.exit(1)
    
    if not (1024 <= args.backend_port <= 65535) or not (1024 <= args.frontend_port <= 65535):
        print("ERROR: Ports must be between 1024 and 65535")
        sys.exit(1)
    
    start_services_with_custom_ports(args.backend_port, args.frontend_port)

if __name__ == "__main__":
    main() 