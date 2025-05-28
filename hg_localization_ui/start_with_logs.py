#!/usr/bin/env python3
"""
Startup script for HG-Localization UI with visible logs
Shows logs from both backend and frontend with prefixes
"""
import subprocess
import sys
import time
import signal
import threading
from pathlib import Path

def stream_output(process, prefix, color_code):
    """Stream output from a process with a prefix"""
    for line in iter(process.stdout.readline, b''):
        if line:
            print(f"\033[{color_code}m[{prefix}]\033[0m {line.decode().rstrip()}")

def start_services():
    """Start both backend and frontend services with visible logs"""
    print(">> Starting HG-Localization UI Services with Logs...")
    
    # Start backend
    print(">> Starting backend...")
    backend_process = subprocess.Popen(
        [sys.executable, "hg_localization_ui/start_ui.py", "--backend"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=False
    )
    
    # Start frontend
    print(">> Starting frontend...")
    frontend_process = subprocess.Popen(
        [sys.executable, "hg_localization_ui/start_ui.py", "--frontend"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=False
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
    print(">> Backend: http://localhost:8000")
    print(">> Frontend: http://localhost:3000")
    print(">> API Docs: http://localhost:8000/docs")
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

if __name__ == "__main__":
    start_services() 