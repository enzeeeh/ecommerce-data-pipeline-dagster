"""
Unit tests for data loading operations.
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

# Import the ops to test
import sys
sys.path.append('..')
from ops.data_loading import load_csv_data
from dagster import build_op_context


class TestDataLoadingOps:
    """Test suite for data loading operations."""
    
    def test_load_csv_data_success(self, sample_csv_file):
        """Test successful CSV data loading."""
        # Create a temporary file in the expected location
        test_data_dir = Path("../test_data")
        test_data_dir.mkdir(exist_ok=True)
        
        test_csv_path = test_data_dir / "Amazon Sale Report.csv"
        
        # Copy sample data to expected location
        with open(sample_csv_file, 'r') as src, open(test_csv_path, 'w') as dst:
            dst.write(src.read())
        
        try:
            # Mock the data loading op to use test path
            context = build_op_context()
            
            # The op should find and validate the file
            result = load_csv_data(context)
            
            # Verify result
            assert result is not None
            assert isinstance(result, str)
            assert Path(result).exists()
            
            # Verify the file can be read as DataFrame
            df = pd.read_csv(result)
            assert len(df) > 0
            assert 'Order ID' in df.columns
            
        finally:
            # Cleanup
            if test_csv_path.exists():
                test_csv_path.unlink()
            if test_data_dir.exists():
                test_data_dir.rmdir()
    
    def test_load_csv_data_file_not_found(self):
        """Test CSV loading when file doesn't exist."""
        context = build_op_context()
        
        # This should raise FileNotFoundError
        with pytest.raises(FileNotFoundError):
            load_csv_data(context)
    
    def test_load_csv_data_invalid_file(self):
        """Test CSV loading with invalid file content."""
        # Create invalid CSV file
        test_data_dir = Path("../test_data")
        test_data_dir.mkdir(exist_ok=True)
        
        test_csv_path = test_data_dir / "Amazon Sale Report.csv"
        
        # Write invalid content
        with open(test_csv_path, 'w') as f:
            f.write("This is not a valid CSV file content")
        
        try:
            context = build_op_context()
            
            # This should raise ValueError
            with pytest.raises(ValueError):
                load_csv_data(context)
                
        finally:
            # Cleanup
            if test_csv_path.exists():
                test_csv_path.unlink()
            if test_data_dir.exists():
                test_data_dir.rmdir()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])