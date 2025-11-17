"""
Unit tests for data processing operations.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

# Import the ops to test
import sys
sys.path.append('..')
from ops.data_processing import clean_amazon_sales_data, insert_raw_data_to_duckdb
from dagster import build_op_context


class TestDataProcessingOps:
    """Test suite for data processing operations."""
    
    def test_clean_amazon_sales_data_success(self, sample_csv_file):
        """Test successful data cleaning operation."""
        context = build_op_context()
        
        # Execute the cleaning op
        result = clean_amazon_sales_data(context, sample_csv_file)
        
        # Verify result
        assert result is not None
        assert isinstance(result, str)
        assert result.endswith("_cleaned.csv")
        assert Path(result).exists()
        
        # Verify cleaned data
        df_cleaned = pd.read_csv(result)
        
        # Check that business rules were applied
        assert len(df_cleaned) == 3  # Same number of records
        
        # Check cancelled order has Amount = 0
        cancelled_orders = df_cleaned[df_cleaned['Status'] == 'Cancelled']
        assert len(cancelled_orders) == 1
        assert cancelled_orders.iloc[0]['Amount'] == 0.0
        
        # Check currency defaults were applied
        assert df_cleaned['currency'].isna().sum() == 0  # No nulls
        assert all(df_cleaned['currency'].fillna('') != '')  # All have values
        
        # Check date conversion
        assert df_cleaned['Date'].dtype.name.startswith('datetime')
        
        # Cleanup
        if Path(result).exists():
            Path(result).unlink()
    
    def test_clean_amazon_sales_data_invalid_file(self):
        """Test data cleaning with invalid file path."""
        context = build_op_context()
        
        # This should raise FileNotFoundError
        with pytest.raises(FileNotFoundError):
            clean_amazon_sales_data(context, "nonexistent_file.csv")
    
    def test_insert_raw_data_to_duckdb_success(self, sample_csv_file, test_duckdb):
        """Test successful data insertion into DuckDB."""
        # First clean the data
        context = build_op_context()
        cleaned_csv = clean_amazon_sales_data(context, sample_csv_file)
        
        # Create mock DuckDB resource
        import duckdb
        from resources.duckdb_resource import DuckDBConnectionWrapper
        
        # Create test database with schema
        conn = duckdb.connect(test_duckdb)
        
        # Create test table
        conn.execute("""
            CREATE TABLE raw_amazon_sales (
                index_id INTEGER,
                order_id VARCHAR,
                date_col DATE,
                category VARCHAR,
                status VARCHAR,
                amount DECIMAL(10,2),
                currency VARCHAR(10),
                ship_state VARCHAR,
                ship_city VARCHAR
            )
        """)
        
        tables = {'raw_data': 'raw_amazon_sales'}
        wrapped_conn = DuckDBConnectionWrapper(conn, tables, test_duckdb)
        
        # Mock context with resource
        context_with_resource = build_op_context(
            resources={'duckdb_resource': wrapped_conn}
        )
        
        try:
            # Execute the insertion op
            result = insert_raw_data_to_duckdb(context_with_resource, cleaned_csv)
            
            # Verify result
            assert isinstance(result, int)
            assert result > 0  # Should have inserted some records
            
            # Verify data was inserted
            count = conn.execute("SELECT COUNT(*) FROM raw_amazon_sales").fetchone()[0]
            assert count == result
            assert count == 3  # Should have 3 test records
            
        finally:
            conn.close()
            # Cleanup
            if Path(cleaned_csv).exists():
                Path(cleaned_csv).unlink()
    
    def test_data_processing_pipeline_integration(self, sample_csv_file, test_duckdb):
        """Test the complete data processing pipeline integration."""
        context = build_op_context()
        
        # Step 1: Clean data
        cleaned_csv = clean_amazon_sales_data(context, sample_csv_file)
        assert Path(cleaned_csv).exists()
        
        # Step 2: Set up database
        import duckdb
        from resources.duckdb_resource import DuckDBConnectionWrapper
        
        conn = duckdb.connect(test_duckdb)
        conn.execute("""
            CREATE TABLE raw_amazon_sales (
                index_id INTEGER,
                order_id VARCHAR,
                date_col DATE,
                category VARCHAR,
                status VARCHAR,
                amount DECIMAL(10,2),
                currency VARCHAR(10),
                ship_state VARCHAR,
                ship_city VARCHAR
            )
        """)
        
        tables = {'raw_data': 'raw_amazon_sales'}
        wrapped_conn = DuckDBConnectionWrapper(conn, tables, test_duckdb)
        
        context_with_resource = build_op_context(
            resources={'duckdb_resource': wrapped_conn}
        )
        
        try:
            # Step 3: Insert data
            records_inserted = insert_raw_data_to_duckdb(context_with_resource, cleaned_csv)
            
            # Verify end-to-end pipeline success
            assert records_inserted == 3
            
            # Verify data quality
            sample_data = conn.execute("SELECT * FROM raw_amazon_sales LIMIT 5").fetchall()
            assert len(sample_data) == 3
            
            # Verify business rules applied
            cancelled_count = conn.execute(
                "SELECT COUNT(*) FROM raw_amazon_sales WHERE status = 'Cancelled' AND amount = 0"
            ).fetchone()[0]
            assert cancelled_count == 1
            
        finally:
            conn.close()
            # Cleanup
            if Path(cleaned_csv).exists():
                Path(cleaned_csv).unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])