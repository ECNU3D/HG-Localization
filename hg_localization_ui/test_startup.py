#!/usr/bin/env python3
"""
Test script to verify that the startup scripts work correctly
"""
import subprocess
import time
import requests
import sys
from pathlib import Path

def test_backend_startup():
    """Test backend startup"""
    print("🔍 Testing backend startup...")
    
    # Start backend in background
    backend_process = subprocess.Popen(
        [sys.executable, "hg_localization_ui/start_ui.py", "--backend"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for backend to start
    print("⏳ Waiting for backend to start...")
    time.sleep(10)
    
    try:
        # Test health endpoint
        response = requests.get("http://localhost:8000/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ Backend started successfully")
            return True
        else:
            print(f"❌ Backend health check failed: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Backend not accessible: {e}")
        return False
    finally:
        # Stop backend
        backend_process.terminate()
        backend_process.wait(timeout=5)

def test_frontend_startup():
    """Test frontend startup"""
    print("\n🔍 Testing frontend startup...")
    
    # Start frontend in background
    frontend_process = subprocess.Popen(
        [sys.executable, "hg_localization_ui/start_ui.py", "--frontend"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for frontend to start
    print("⏳ Waiting for frontend to start...")
    time.sleep(15)  # Frontend takes longer to start
    
    try:
        # Test frontend endpoint
        response = requests.get("http://localhost:3000", timeout=10)
        if response.status_code == 200 and "html" in response.text.lower():
            print("✅ Frontend started successfully")
            return True
        else:
            print(f"❌ Frontend check failed: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Frontend not accessible: {e}")
        return False
    finally:
        # Stop frontend
        frontend_process.terminate()
        try:
            frontend_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            frontend_process.kill()

def main():
    """Run startup tests"""
    print("=" * 60)
    print("🧪 HG-Localization UI Startup Test")
    print("=" * 60)
    
    backend_ok = test_backend_startup()
    frontend_ok = test_frontend_startup()
    
    print("\n" + "=" * 60)
    print("📊 Startup Test Results")
    print("=" * 60)
    
    print(f"{'✅ PASS' if backend_ok else '❌ FAIL'} Backend Startup")
    print(f"{'✅ PASS' if frontend_ok else '❌ FAIL'} Frontend Startup")
    
    if backend_ok and frontend_ok:
        print("\n🎉 All startup tests passed!")
        print("\n🚀 You can now start the full application with:")
        print("   python hg_localization_ui/start_ui.py")
        return True
    else:
        print("\n⚠️  Some startup tests failed. Check the error messages above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 