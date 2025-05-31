#!/usr/bin/env python3
"""
Startup script for HG-Localization UI with visible logs and custom ports
Shows logs from both backend and frontend with prefixes
Supports custom port configuration
"""
import subprocess
import sys
import time
import signal
import threading
import argparse
import os
from pathlib import Path

def stream_output(process, prefix, color_code):
    """Stream output from a process with a prefix"""
    for line in iter(process.stdout.readline, b''):
        if line:
            print(f"\033[{color_code}m[{prefix}]\033[0m {line.decode().rstrip()}")

def start_services(backend_port=8000, frontend_port=3000):
    """Start both backend and frontend services with visible logs"""
    print(f">> Starting HG-Localization UI Services with Logs...")
    print(f">> Backend Port: {backend_port}")
    print(f">> Frontend Port: {frontend_port}")
    
    # Update CORS settings in environment
    os.environ['HGLOC_BACKEND_PORT'] = str(backend_port)
    os.environ['HGLOC_FRONTEND_PORT'] = str(frontend_port)
    
    # Start backend with custom port
    print(">> Starting backend...")
    backend_process = subprocess.Popen(
        [sys.executable, "-c", f"""
import sys
import os
from pathlib import Path

# Add paths
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / 'hg_localization_ui' / 'backend'))

# Change to backend directory
os.chdir(Path(__file__).parent / 'hg_localization_ui' / 'backend')

import uvicorn
from main import app

# Update CORS origins for custom frontend port
from fastapi.middleware.cors import CORSMiddleware
app.user_middleware.clear()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:{frontend_port}", "http://127.0.0.1:{frontend_port}"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

uvicorn.run(
    app,
    host="0.0.0.0",
    port={backend_port},
    reload=False,
    log_level="info"
)
"""],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=False,
        cwd=Path(__file__).parent.parent
    )
    
    # Start frontend with custom port
    print(">> Starting frontend...")
    frontend_env = os.environ.copy()
    frontend_env['PORT'] = str(frontend_port)
    frontend_env['REACT_APP_API_URL'] = f'http://localhost:{backend_port}/api'
    
    frontend_process = subprocess.Popen(
        ["npm", "start"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=False,
        cwd=Path(__file__).parent / "frontend",
        env=frontend_env,
        shell=True
    )
    
    # Start log streaming threads
    backend_thread = threading.Thread(
        target=stream_output, 
        args=(backend_process, "BACKEND", "34"),  # Blue
        daemon=True
    )
    frontend_thread = threading.Thread(
        target=stream_output, 
        args=(frontend_process, "FRONTEND", "32"),  # Green
        daemon=True
    )
    
    backend_thread.start()
    frontend_thread.start()
    
    print("OK Services starting...")
    print(f">> Backend: http://localhost:{backend_port}")
    print(f">> Frontend: http://localhost:{frontend_port}")
    print(f">> API Docs: http://localhost:{backend_port}/docs")
    print("\nLogs will appear below with [BACKEND] and [FRONTEND] prefixes")
    print("Press Ctrl+C to stop all services\n")
    
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
  python start_with_logs_custom_ports.py                    # Default ports (8000, 3000)
  python start_with_logs_custom_ports.py --backend-port 8001 --frontend-port 3001
  python start_with_logs_custom_ports.py -b 9000 -f 4000
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
    
    print("=" * 60)
    print(">> HG-Localization UI Startup Script (Custom Ports)")
    print("=" * 60)
    
    start_services(args.backend_port, args.frontend_port)

if __name__ == "__main__":
    main() 