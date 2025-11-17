"""
Pipeline Execution Tests
Tests for verifying the complete pipeline execution and analytics table creation.
"""

import os
import sys
import pytest
import duckdb
from pathlib import Path

# Add the dagster_project directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dagster import execute_job
from jobs.data_pipeline import amazon_sales_pipeline


class TestPipelineExecution:
    """Test complete pipeline execution and results verification."""
    
    def test_complete_pipeline_execution(self):
        """Test the complete pipeline execution with real data."""
        print("\n" + "="*60)
        print("🚀 TESTING COMPLETE PIPELINE EXECUTION")
        print("="*60)
        
        # Execute the complete pipeline
        result = execute_job(amazon_sales_pipeline)
        
        # Verify the job succeeded
        assert result.success, f"Pipeline execution failed: {result}"
        print("✅ Pipeline executed successfully!")
        
        # Verify all ops succeeded
        expected_ops = [
            'load_csv_data',
            'clean_amazon_sales_data', 
            'insert_raw_data_to_duckdb',
            'create_monthly_revenue_table',
            'create_daily_orders_table'
        ]
        
        for op_name in expected_ops:
            op_result = result.result_for_node(op_name)
            assert op_result.success, f"Op {op_name} failed"
            print(f"✅ Op '{op_name}' completed successfully")
    
    def test_database_tables_created(self):
        """Test that all expected database tables are created with correct data."""
        print("\n" + "="*60)
        print("🔍 TESTING DATABASE TABLES CREATION")
        print("="*60)
        
        # Check if database file exists
        db_path = Path(__file__).parent.parent.parent / "data" / "sales.duckdb"
        assert db_path.exists(), f"Database file not found at {db_path}"
        print(f"✅ Database file exists: {db_path}")
        
        # Connect to database and verify tables
        conn = duckdb.connect(str(db_path))
        
        try:
            # Get all tables
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            expected_tables = ['raw_amazon_sales', 'monthly_revenue', 'daily_orders']
            
            print(f"📋 Tables found: {table_names}")
            
            for expected_table in expected_tables:
                assert expected_table in table_names, f"Table {expected_table} not found"
                print(f"✅ Table '{expected_table}' exists")
            
            # Test each table has data
            self._test_raw_amazon_sales_table(conn)
            self._test_monthly_revenue_table(conn)
            self._test_daily_orders_table(conn)
            
        finally:
            conn.close()
    
    def _test_raw_amazon_sales_table(self, conn):
        """Test raw Amazon sales table."""
        print("\n" + "-"*40)
        print("📊 TESTING RAW AMAZON SALES TABLE")
        print("-"*40)
        
        # Count total records
        count = conn.execute("SELECT COUNT(*) FROM raw_amazon_sales").fetchone()[0]
        assert count > 0, "raw_amazon_sales table is empty"
        print(f"✅ Raw Amazon Sales: {count:,} records")
        
        # Check for required columns
        columns = conn.execute("PRAGMA table_info(raw_amazon_sales)").fetchall()
        column_names = [col[1] for col in columns]
        
        required_columns = ['Order_ID', 'Date', 'Status', 'Amount', 'Category']
        for col in required_columns:
            assert col in column_names, f"Column {col} missing from raw_amazon_sales"
        
        print(f"✅ All required columns present: {required_columns}")
        
        # Sample a few records
        sample = conn.execute("SELECT * FROM raw_amazon_sales LIMIT 3").fetchall()
        print(f"📋 Sample records: {len(sample)} rows shown")
        for i, record in enumerate(sample, 1):
            print(f"   Record {i}: Order_ID={record[0]}, Status={record[3]}, Amount={record[4]}")
    
    def _test_monthly_revenue_table(self, conn):
        """Test monthly revenue table."""
        print("\n" + "-"*40)
        print("💰 TESTING MONTHLY REVENUE TABLE")
        print("-"*40)
        
        # Count records
        count = conn.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0]
        assert count > 0, "monthly_revenue table is empty"
        print(f"✅ Monthly Revenue: {count} records")
        
        # Check columns
        columns = conn.execute("PRAGMA table_info(monthly_revenue)").fetchall()
        column_names = [col[1] for col in columns]
        
        expected_columns = ['year_month', 'category', 'total_revenue', 'order_count']
        for col in expected_columns:
            assert col in column_names, f"Column {col} missing from monthly_revenue"
        
        print(f"✅ All required columns present: {expected_columns}")
        
        # Get top performing month/category
        top_revenue = conn.execute("""
            SELECT year_month, category, total_revenue, order_count 
            FROM monthly_revenue 
            ORDER BY total_revenue DESC 
            LIMIT 5
        """).fetchall()
        
        print("🏆 Top 5 Month/Category by Revenue:")
        for i, (month, category, revenue, orders) in enumerate(top_revenue, 1):
            print(f"   {i}. {month} - {category}: ${revenue:,.2f} ({orders:,} orders)")
    
    def _test_daily_orders_table(self, conn):
        """Test daily orders table."""
        print("\n" + "-"*40)
        print("📅 TESTING DAILY ORDERS TABLE")
        print("-"*40)
        
        # Count records
        count = conn.execute("SELECT COUNT(*) FROM daily_orders").fetchone()[0]
        print(f"📊 Daily Orders: {count} records")
        
        # This was the main issue - let's make sure it's not 0
        assert count > 0, "daily_orders table is empty - this was the main bug!"
        print("✅ Daily orders table has data (bug fixed!)")
        
        # Check columns
        columns = conn.execute("PRAGMA table_info(daily_orders)").fetchall()
        column_names = [col[1] for col in columns]
        
        expected_columns = ['order_date', 'status', 'daily_revenue', 'order_count']
        for col in expected_columns:
            assert col in column_names, f"Column {col} missing from daily_orders"
        
        print(f"✅ All required columns present: {expected_columns}")
        
        # Get date range
        date_range = conn.execute("""
            SELECT MIN(order_date) as min_date, MAX(order_date) as max_date,
                   COUNT(DISTINCT order_date) as unique_dates,
                   COUNT(DISTINCT status) as unique_statuses
            FROM daily_orders
        """).fetchone()
        
        min_date, max_date, unique_dates, unique_statuses = date_range
        print(f"📅 Date range: {min_date} to {max_date}")
        print(f"📊 Unique dates: {unique_dates}, Unique statuses: {unique_statuses}")
        
        # Get top revenue days
        top_days = conn.execute("""
            SELECT order_date, status, daily_revenue, order_count
            FROM daily_orders 
            ORDER BY daily_revenue DESC 
            LIMIT 5
        """).fetchall()
        
        print("🏆 Top 5 Days by Revenue:")
        for i, (date, status, revenue, orders) in enumerate(top_days, 1):
            print(f"   {i}. {date} ({status}): ${revenue:,.2f} ({orders} orders)")
        
        # Verify total revenue
        total_revenue = conn.execute("SELECT SUM(daily_revenue) FROM daily_orders").fetchone()[0]
        total_orders = conn.execute("SELECT SUM(order_count) FROM daily_orders").fetchone()[0]
        print(f"💰 Total tracked revenue: ${total_revenue:,.2f}")
        print(f"📦 Total tracked orders: {total_orders:,}")


class TestAnalyticsOps:
    """Test individual analytics operations."""
    
    def test_monthly_revenue_sql_logic(self):
        """Test the monthly revenue aggregation logic."""
        # This test would verify the SQL logic is correct
        # by creating sample data and testing the query
        pass
    
    def test_daily_orders_sql_logic(self):
        """Test the daily orders aggregation logic."""
        # This test would verify the daily orders SQL is correct
        # and handles NULL values properly
        pass


if __name__ == "__main__":
    # Run tests directly if this file is executed
    print("🧪 Running Pipeline Execution Tests...")
    
    test_instance = TestPipelineExecution()
    
    try:
        # Test complete pipeline
        test_instance.test_complete_pipeline_execution()
        
        # Test database results
        test_instance.test_database_tables_created()
        
        print("\n" + "="*60)
        print("🎉 ALL TESTS PASSED! Pipeline is working correctly!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise