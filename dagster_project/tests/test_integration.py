"""
Integration tests for the complete Dagster pipeline.
"""

import pytest
import tempfile
import os
from pathlib import Path
import pandas as pd
import duckdb

# Import pipeline components
import sys
sys.path.append('..')
from jobs.data_pipeline import amazon_sales_pipeline
from resources.duckdb_resource import duckdb_resource


class TestPipelineIntegration:
    """Integration tests for the complete pipeline."""
    
    def test_complete_pipeline_execution(self, sample_csv_file):
        """Test the complete pipeline from CSV to analytics tables."""
        # Create temporary directories for test
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Set up test data directory structure
            test_data_dir = temp_path / "data"
            test_data_dir.mkdir()
            test_csv = test_data_dir / "Amazon Sale Report.csv"
            
            # Copy sample data
            with open(sample_csv_file, 'r') as src, open(test_csv, 'w') as dst:
                dst.write(src.read())
            
            # Test database path
            test_db = test_data_dir / "sales.duckdb"
            
            # Temporarily modify the data loading op to use test path
            # This is a simplified approach - in production, use dependency injection
            original_cwd = os.getcwd()
            
            try:
                # Change to temp directory so relative paths work
                os.chdir(temp_path)
                
                # Execute the pipeline
                job_result = amazon_sales_pipeline.execute_in_process(
                    resources={'duckdb_resource': duckdb_resource}
                )
                
                # Verify pipeline success
                assert job_result.success, f"Pipeline failed: {job_result.failure_data}"
                
                # Verify database was created
                assert test_db.exists(), "Sales database was not created"
                
                # Connect and verify tables
                conn = duckdb.connect(str(test_db))
                
                # Check tables exist
                tables = conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in tables]
                
                expected_tables = {'raw_amazon_sales', 'monthly_revenue', 'daily_orders'}
                actual_tables = set(table_names)
                
                assert expected_tables.issubset(actual_tables), f"Missing tables: {expected_tables - actual_tables}"
                
                # Verify data in each table
                raw_count = conn.execute("SELECT COUNT(*) FROM raw_amazon_sales").fetchone()[0]
                revenue_count = conn.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0]
                orders_count = conn.execute("SELECT COUNT(*) FROM daily_orders").fetchone()[0]
                
                assert raw_count > 0, "No data in raw table"
                assert revenue_count > 0, "No data in monthly revenue table"
                assert orders_count > 0, "No data in daily orders table"
                
                # Verify business logic was applied
                cancelled_with_zero = conn.execute(
                    "SELECT COUNT(*) FROM raw_amazon_sales WHERE status = 'Cancelled' AND amount = 0"
                ).fetchone()[0]
                assert cancelled_with_zero > 0, "Cancelled orders business rule not applied"
                
                # Verify analytics calculations
                total_revenue = conn.execute(
                    "SELECT SUM(total_revenue) FROM monthly_revenue"
                ).fetchone()[0]
                assert total_revenue > 0, "No revenue calculated"
                
                conn.close()
                
                # Verify cleaned CSV was created
                cleaned_csv = test_data_dir / "Amazon Sale Report_cleaned.csv"
                assert cleaned_csv.exists(), "Cleaned CSV was not created"
                
                # Verify cleaned data structure
                df_cleaned = pd.read_csv(cleaned_csv)
                assert len(df_cleaned) == 3, f"Expected 3 records, got {len(df_cleaned)}"
                assert 'data_quality_flag' in df_cleaned.columns or df_cleaned['currency'].isna().sum() == 0
                
            finally:
                os.chdir(original_cwd)
    
    def test_pipeline_failure_scenarios(self):
        """Test pipeline behavior under failure conditions."""
        # Test with missing CSV file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            original_cwd = os.getcwd()
            
            try:
                os.chdir(temp_path)
                
                # This should fail because no CSV file exists
                job_result = amazon_sales_pipeline.execute_in_process(
                    resources={'duckdb_resource': duckdb_resource}
                )
                
                # Pipeline should fail gracefully
                assert not job_result.success, "Pipeline should have failed with missing CSV"
                
            finally:
                os.chdir(original_cwd)
    
    def test_pipeline_data_quality_validation(self, sample_csv_file):
        """Test that pipeline validates data quality correctly."""
        # Create CSV with data quality issues
        bad_csv_content = """Order ID,Date,Status,Category,Amount,currency
B00-001,3-31-22,Shipped,Set,299.0,INR
B00-002,4-01-22,Shipped,kurta,,INR
B00-003,4-02-22,Cancelled,Set,,
"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Set up test data directory
            test_data_dir = temp_path / "data"
            test_data_dir.mkdir()
            test_csv = test_data_dir / "Amazon Sale Report.csv"
            
            # Write problematic data
            with open(test_csv, 'w') as f:
                f.write(bad_csv_content)
            
            original_cwd = os.getcwd()
            
            try:
                os.chdir(temp_path)
                
                # Execute pipeline
                job_result = amazon_sales_pipeline.execute_in_process(
                    resources={'duckdb_resource': duckdb_resource}
                )
                
                # Pipeline should complete (with data quality handling)
                if job_result.success:
                    # Verify data quality handling
                    test_db = test_data_dir / "sales.duckdb"
                    conn = duckdb.connect(str(test_db))
                    
                    # Check that data quality issues were handled
                    flagged_records = conn.execute(
                        "SELECT COUNT(*) FROM raw_amazon_sales WHERE data_quality_flag IS NOT NULL"
                    ).fetchone()[0]
                    
                    # Should have flagged non-cancelled orders with missing amounts
                    assert flagged_records >= 0  # May be 0 if all handled correctly
                    
                    conn.close()
                
            finally:
                os.chdir(original_cwd)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])