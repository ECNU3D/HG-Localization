#!/usr/bin/env python3
"""
Simple startup script for HG-Localization UI
Uses subprocess to avoid threading issues with uvicorn
"""
import subprocess
import sys
import time
import signal
import os
from pathlib import Path

def start_services():
    """Start both backend and frontend services"""
    print("🚀 Starting HG-Localization UI Services...")
    
    # Start backend
    print("📡 Starting backend...")
    backend_process = subprocess.Popen(
        [sys.executable, "hg_localization_ui/start_ui.py", "--backend"]
        # Note: Not capturing stdout/stderr so backend logs are visible
    )
    
    # Wait for backend to start
    time.sleep(5)
    
    # Start frontend
    print("🌐 Starting frontend...")
    frontend_process = subprocess.Popen(
        [sys.executable, "hg_localization_ui/start_ui.py", "--frontend"]
        # Note: Not capturing stdout/stderr so frontend logs are visible
    )
    
    print("✅ Services starting...")
    print("📡 Backend: http://localhost:8000")
    print("🌐 Frontend: http://localhost:3000")
    print("📚 API Docs: http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop all services")
    
    def signal_handler(sig, frame):
        print("\n🛑 Stopping services...")
        backend_process.terminate()
        frontend_process.terminate()
        
        # Wait for processes to stop
        try:
            backend_process.wait(timeout=5)
            frontend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            backend_process.kill()
            frontend_process.kill()
        
        print("✅ All services stopped")
        sys.exit(0)
    
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Wait for processes
        while True:
            if backend_process.poll() is not None:
                print("❌ Backend process stopped unexpectedly")
                break
            if frontend_process.poll() is not None:
                print("❌ Frontend process stopped unexpectedly")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    start_services() 