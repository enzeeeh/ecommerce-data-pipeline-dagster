#!/usr/bin/env python3
"""
Setup script for Amazon Sales Data Pipeline
Automates environment setup and dependency installation
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        return False
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def setup_environment():
    """Set up the development environment"""
    print("🚀 Setting up Amazon Sales Data Pipeline Environment")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        return False
    
    # Upgrade pip
    if not run_command("python -m pip install --upgrade pip", "Upgrading pip"):
        return False
    
    # Install production dependencies
    if not run_command("pip install -r requirements.txt", "Installing production dependencies"):
        return False
    
    # Ask if user wants development dependencies
    install_dev = input("\n🤔 Install development dependencies (testing, linting, etc.)? [y/N]: ").lower().startswith('y')
    
    if install_dev:
        if not run_command("pip install -r requirements-dev.txt", "Installing development dependencies"):
            return False
    
    # Create necessary directories
    directories = [
        "dagster_project/.dagster_home",
        "data",
        "logs",
        "htmlcov"
    ]
    
    for dir_path in directories:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"📁 Created directory: {dir_path}")
    
    print(f"\n🎉 Environment setup completed successfully!")
    print(f"\n📋 Next Steps:")
    print(f"1. Run the data pipeline: cd dagster_project && python run_pipeline.py")
    print(f"2. Start Dagster UI: cd dagster_project && dagster dev")
    print(f"3. Run analysis: jupyter notebook notebooks/analysis.ipynb")
    print(f"4. Run tests: pytest")
    
    return True

if __name__ == "__main__":
    success = setup_environment()
    sys.exit(0 if success else 1)