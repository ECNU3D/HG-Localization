#!/usr/bin/env python3
"""
Startup script for HG-Localization UI Frontend
"""
import os
import subprocess
import sys
from pathlib import Path

def main():
    """Start the React frontend development server"""
    print("Starting HG-Localization UI Frontend...")
    print("Frontend will be available at: http://localhost:3000")
    print("Press Ctrl+C to stop the server")
    
    # Change to frontend directory
    frontend_dir = Path(__file__).parent / "frontend"
    
    if not frontend_dir.exists():
        print(f"Error: Frontend directory not found at {frontend_dir}")
        sys.exit(1)
    
    # Check if node_modules exists
    node_modules = frontend_dir / "node_modules"
    if not node_modules.exists():
        print("Installing frontend dependencies...")
        subprocess.run(["npm", "install"], cwd=frontend_dir, check=True, shell=True)
    
    # Start the development server
    try:
        subprocess.run(["npm", "start"], cwd=frontend_dir, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error starting frontend: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nFrontend server stopped.")

if __name__ == "__main__":
    main() 