"""
Unit tests for analytics operations.
"""

import pytest
import pandas as pd
import duckdb

# Import the ops to test
import sys
sys.path.append('..')
from ops.analytics import create_monthly_revenue_table, create_daily_orders_table
from resources.duckdb_resource import DuckDBConnectionWrapper
from dagster import build_op_context


class TestAnalyticsOps:
    """Test suite for analytics operations."""
    
    def setup_test_database(self, test_duckdb):
        """Set up test database with sample data."""
        conn = duckdb.connect(test_duckdb)
        
        # Create raw data table
        conn.execute("""
            CREATE TABLE raw_amazon_sales (
                order_id VARCHAR,
                date_col DATE,
                category VARCHAR,
                status VARCHAR,
                amount DECIMAL(10,2),
                qty INTEGER
            )
        """)
        
        # Insert test data
        test_data = [
            ('B00-001', '2022-04-01', 'Set', 'Shipped', 299.0, 1),
            ('B00-002', '2022-04-01', 'kurta', 'Shipped', 599.5, 2),
            ('B00-003', '2022-04-02', 'Set', 'Cancelled', 0.0, 1),
            ('B00-004', '2022-04-15', 'Western Dress', 'Shipped', 799.0, 1),
            ('B00-005', '2022-05-01', 'Set', 'Shipped', 399.0, 1),
            ('B00-006', '2022-05-01', 'kurta', 'Cancelled', 0.0, 1),
        ]
        
        for row in test_data:
            conn.execute(
                "INSERT INTO raw_amazon_sales VALUES (?, ?, ?, ?, ?, ?)", 
                row
            )
        
        # Create analytics tables
        conn.execute("""
            CREATE TABLE monthly_revenue (
                year_month VARCHAR,
                category VARCHAR,
                total_revenue DECIMAL(12,2),
                order_count INTEGER,
                avg_order_value DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.execute("""
            CREATE TABLE daily_orders (
                order_date DATE,
                status VARCHAR,
                order_count INTEGER,
                total_quantity INTEGER,
                total_amount DECIMAL(12,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        return conn
    
    def test_create_monthly_revenue_table_success(self, test_duckdb):
        """Test successful monthly revenue table creation."""
        conn = self.setup_test_database(test_duckdb)
        
        tables = {
            'raw_data': 'raw_amazon_sales',
            'monthly_revenue': 'monthly_revenue',
            'daily_orders': 'daily_orders'
        }
        wrapped_conn = DuckDBConnectionWrapper(conn, tables, test_duckdb)
        
        context = build_op_context(
            resources={'duckdb_resource': wrapped_conn}
        )
        
        try:
            # Execute the op
            result = create_monthly_revenue_table(context, 6)  # 6 records inserted
            
            # Verify result
            assert isinstance(result, int)
            assert result > 0
            
            # Verify data was created
            revenue_data = conn.execute("SELECT * FROM monthly_revenue ORDER BY year_month, total_revenue DESC").fetchall()
            assert len(revenue_data) > 0
            
            # Verify data structure
            assert len(revenue_data[0]) == 6  # 6 columns expected
            
            # Verify business logic (cancelled orders excluded)
            april_set = conn.execute(
                "SELECT total_revenue FROM monthly_revenue WHERE year_month = '2022-04' AND category = 'Set'"
            ).fetchone()
            assert april_set[0] == 299.0  # Only non-cancelled order
            
        finally:
            conn.close()
    
    def test_create_daily_orders_table_success(self, test_duckdb):
        """Test successful daily orders table creation."""
        conn = self.setup_test_database(test_duckdb)
        
        tables = {
            'raw_data': 'raw_amazon_sales',
            'monthly_revenue': 'monthly_revenue',
            'daily_orders': 'daily_orders'
        }
        wrapped_conn = DuckDBConnectionWrapper(conn, tables, test_duckdb)
        
        context = build_op_context(
            resources={'duckdb_resource': wrapped_conn}
        )
        
        try:
            # Execute the op
            result = create_daily_orders_table(context, 6)  # 6 records inserted
            
            # Verify result
            assert isinstance(result, int)
            assert result > 0
            
            # Verify data was created
            orders_data = conn.execute("SELECT * FROM daily_orders ORDER BY order_date, status").fetchall()
            assert len(orders_data) > 0
            
            # Verify data structure
            assert len(orders_data[0]) == 6  # 6 columns expected
            
            # Verify aggregation logic
            april_1_shipped = conn.execute(
                "SELECT order_count, total_quantity FROM daily_orders WHERE order_date = '2022-04-01' AND status = 'Shipped'"
            ).fetchone()
            assert april_1_shipped[0] == 2  # 2 shipped orders on April 1
            assert april_1_shipped[1] == 3  # Total quantity 1 + 2 = 3
            
        finally:
            conn.close()
    
    def test_analytics_ops_integration(self, test_duckdb):
        """Test both analytics ops working together."""
        conn = self.setup_test_database(test_duckdb)
        
        tables = {
            'raw_data': 'raw_amazon_sales',
            'monthly_revenue': 'monthly_revenue',
            'daily_orders': 'daily_orders'
        }
        wrapped_conn = DuckDBConnectionWrapper(conn, tables, test_duckdb)
        
        context = build_op_context(
            resources={'duckdb_resource': wrapped_conn}
        )
        
        try:
            # Execute both ops
            revenue_result = create_monthly_revenue_table(context, 6)
            orders_result = create_daily_orders_table(context, 6)
            
            # Verify both succeeded
            assert revenue_result > 0
            assert orders_result > 0
            
            # Verify cross-table consistency
            total_revenue_from_monthly = conn.execute(
                "SELECT SUM(total_revenue) FROM monthly_revenue"
            ).fetchone()[0]
            
            total_revenue_from_daily = conn.execute(
                "SELECT SUM(total_amount) FROM daily_orders WHERE status != 'Cancelled'"
            ).fetchone()[0]
            
            # Should be approximately equal (accounting for cancelled orders)
            assert abs(total_revenue_from_monthly - total_revenue_from_daily) < 1.0
            
        finally:
            conn.close()
    
    def test_analytics_ops_empty_data(self, test_duckdb):
        """Test analytics ops with empty raw data."""
        conn = duckdb.connect(test_duckdb)
        
        # Create empty tables
        conn.execute("""
            CREATE TABLE raw_amazon_sales (
                order_id VARCHAR,
                date_col DATE,
                category VARCHAR,
                status VARCHAR,
                amount DECIMAL(10,2),
                qty INTEGER
            )
        """)
        
        conn.execute("""
            CREATE TABLE monthly_revenue (
                year_month VARCHAR,
                category VARCHAR,
                total_revenue DECIMAL(12,2),
                order_count INTEGER,
                avg_order_value DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        tables = {
            'raw_data': 'raw_amazon_sales',
            'monthly_revenue': 'monthly_revenue'
        }
        wrapped_conn = DuckDBConnectionWrapper(conn, tables, test_duckdb)
        
        context = build_op_context(
            resources={'duckdb_resource': wrapped_conn}
        )
        
        try:
            # Execute the op with empty data
            result = create_monthly_revenue_table(context, 0)
            
            # Should complete successfully even with no data
            assert isinstance(result, int)
            assert result == 0
            
        finally:
            conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])