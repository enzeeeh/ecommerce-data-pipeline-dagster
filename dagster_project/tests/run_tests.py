"""
Test runner script for all pipeline tests.
"""

import pytest
import sys
import os
from pathlib import Path

def run_all_tests():
    """Run all tests and generate coverage report."""
    print("🧪 Running Amazon Sales Pipeline Test Suite")
    print("=" * 50)
    
    # Add current directory to Python path
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir.parent))
    
    # Test configuration
    test_args = [
        str(current_dir),  # Test directory
        "-v",              # Verbose output
        "-s",              # Don't capture stdout (for debugging)
        "--tb=short",      # Short traceback format
        "--color=yes",     # Colored output
        "-x",              # Stop on first failure
    ]
    
    print(f"📁 Test directory: {current_dir}")
    print(f"🎯 Running tests with args: {' '.join(test_args)}")
    print()
    
    # Run tests
    exit_code = pytest.main(test_args)
    
    if exit_code == 0:
        print("\n✅ ALL TESTS PASSED!")
        print("🎉 Pipeline is ready for production use!")
    else:
        print(f"\n❌ Tests failed with exit code: {exit_code}")
        print("🔍 Please review test output and fix issues.")
    
    return exit_code

def run_specific_test_suite(suite_name):
    """Run a specific test suite."""
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir.parent))
    
    test_files = {
        'loading': 'test_data_loading.py',
        'processing': 'test_data_processing.py', 
        'analytics': 'test_analytics.py',
        'integration': 'test_integration.py'
    }
    
    if suite_name not in test_files:
        print(f"❌ Unknown test suite: {suite_name}")
        print(f"Available suites: {', '.join(test_files.keys())}")
        return 1
    
    test_file = current_dir / test_files[suite_name]
    
    print(f"🧪 Running {suite_name} tests")
    print(f"📁 File: {test_file}")
    print("=" * 40)
    
    exit_code = pytest.main([str(test_file), "-v", "-s"])
    
    if exit_code == 0:
        print(f"\n✅ {suite_name.upper()} TESTS PASSED!")
    else:
        print(f"\n❌ {suite_name.upper()} tests failed!")
    
    return exit_code

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test suite
        suite_name = sys.argv[1]
        exit_code = run_specific_test_suite(suite_name)
    else:
        # Run all tests
        exit_code = run_all_tests()
    
    sys.exit(exit_code)