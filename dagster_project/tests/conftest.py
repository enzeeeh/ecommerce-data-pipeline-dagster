"""
Test configuration and fixtures for Dagster pipeline tests.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
import duckdb

# Test data sample
SAMPLE_CSV_DATA = """Order ID,Date,Status,Fulfilment,Sales Channel ,ship-service-level,Style,SKU,Category,Size,ASIN,Courier Status,Qty,currency,Amount,ship-city,ship-state,ship-postal-code,ship-country,promotion-ids
B00-1234567-1234567,3-31-22,Shipped,Amazon,Amazon.in,Standard,Shirt,ABC-123,Set,M,B08ABC123,Shipped,1,INR,299.0,BENGALURU,KARNATAKA,560001,IN,
B00-1234567-1234568,4-01-22,Cancelled,Amazon,Amazon.in,Expedited,Dress,DEF-456,kurta,L,B08DEF456,Cancelled,1,INR,,MUMBAI,MAHARASHTRA,400001,IN,LIGHTNING
B00-1234567-1234569,4-02-22,Shipped,Amazon,Amazon.in,Standard,Top,GHI-789,Western Dress,S,B08GHI789,Shipped,2,INR,599.5,DELHI,DELHI,110001,IN,"""

@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file with sample data for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(SAMPLE_CSV_DATA)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)

@pytest.fixture
def test_duckdb():
    """Create a temporary DuckDB database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as f:
        temp_db_path = f.name
    
    yield temp_db_path
    
    # Cleanup
    if os.path.exists(temp_db_path):
        os.unlink(temp_db_path)

@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing data processing ops."""
    return pd.DataFrame({
        'Order ID': ['B00-1234567-1', 'B00-1234567-2', 'B00-1234567-3'],
        'Date': ['3-31-22', '4-01-22', '4-02-22'],
        'Status': ['Shipped', 'Cancelled', 'Shipped'],
        'Category': ['Set', 'kurta', 'Western Dress'],
        'Amount': [299.0, None, 599.5],
        'currency': ['INR', None, 'INR'],
        'ship-state': ['KARNATAKA', 'MAHARASHTRA', 'DELHI'],
        'ship-city': ['BENGALURU', 'MUMBAI', 'DELHI']
    })