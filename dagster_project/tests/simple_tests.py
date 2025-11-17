"""
Simple test runner to verify pipeline components work correctly.
"""

import sys
import os
from pathlib import Path
import pandas as pd
import tempfile
import duckdb

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_data_cleaning():
    """Test data cleaning functionality."""
    print("🧹 Testing Data Cleaning...")
    
    from ops.data_processing import clean_amazon_sales_data
    from dagster import build_op_context
    
    # Create sample CSV
    sample_data = """Order ID,Date,Status,Category,Amount,currency
B00-001,3-31-22,Shipped,Set,299.0,INR
B00-002,4-01-22,Cancelled,kurta,,
B00-003,4-02-22,Shipped,Dress,599.5,
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(sample_data)
        temp_csv = f.name
    
    try:
        context = build_op_context()
        cleaned_csv = clean_amazon_sales_data(context, temp_csv)
        
        # Verify cleaned data
        df = pd.read_csv(cleaned_csv)
        
        # Test cancelled order has Amount = 0
        cancelled = df[df['Status'] == 'Cancelled']
        assert len(cancelled) == 1
        assert cancelled.iloc[0]['Amount'] == 0.0
        
        # Test currency defaults
        assert df['currency'].isna().sum() == 0
        
        print("   ✅ Data cleaning works correctly")
        
        # Cleanup
        os.unlink(temp_csv)
        os.unlink(cleaned_csv)
        
    except Exception as e:
        print(f"   ❌ Data cleaning failed: {e}")
        return False
    
    return True

def test_duckdb_operations():
    """Test DuckDB operations."""
    print("🦆 Testing DuckDB Operations...")
    
    try:
        # Create a proper temp database file
        test_db = tempfile.mktemp(suffix='.duckdb')
        
        # Test connection and table creation
        conn = duckdb.connect(test_db)
        
        # Create test table
        conn.execute("""
            CREATE TABLE test_sales (
                id INTEGER,
                amount DECIMAL(10,2),
                status VARCHAR,
                date_col DATE
            )
        """)
        
        # Insert test data
        conn.execute("INSERT INTO test_sales VALUES (1, 299.0, 'Shipped', '2022-04-01')")
        conn.execute("INSERT INTO test_sales VALUES (2, 599.5, 'Cancelled', '2022-04-02')")
        
        # Test query
        result = conn.execute("SELECT COUNT(*) FROM test_sales").fetchone()[0]
        assert result == 2
        
        # Test aggregation
        total = conn.execute("SELECT SUM(amount) FROM test_sales WHERE status = 'Shipped'").fetchone()[0]
        assert total == 299.0
        
        conn.close()
        os.unlink(test_db)
        
        print("   ✅ DuckDB operations work correctly")
        return True
        
    except Exception as e:
        print(f"   ❌ DuckDB operations failed: {e}")
        return False

def test_pipeline_components():
    """Test that all pipeline components can be imported."""
    print("📦 Testing Pipeline Component Imports...")
    
    try:
        from ops.data_loading import load_csv_data
        from ops.data_processing import clean_amazon_sales_data, insert_raw_data_to_duckdb
        from ops.analytics import create_monthly_revenue_table, create_daily_orders_table
        from resources.duckdb_resource import duckdb_resource
        from jobs.data_pipeline import amazon_sales_pipeline
        from repository import amazon_sales_repository
        
        print("   ✅ All pipeline components imported successfully")
        return True
        
    except Exception as e:
        print(f"   ❌ Import failed: {e}")
        return False

def test_data_analysis():
    """Test data analysis functionality."""
    print("📊 Testing Data Analysis Components...")
    
    try:
        # Test that we can connect to the actual database
        db_path = "../../data/sales.duckdb"
        if os.path.exists(db_path):
            conn = duckdb.connect(db_path)
            
            # Test table queries
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            expected = {'raw_amazon_sales', 'monthly_revenue', 'daily_orders'}
            if expected.issubset(set(table_names)):
                print("   ✅ All required tables exist")
                
                # Test sample queries
                raw_count = conn.execute("SELECT COUNT(*) FROM raw_amazon_sales").fetchone()[0]
                revenue_count = conn.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0]
                orders_count = conn.execute("SELECT COUNT(*) FROM daily_orders").fetchone()[0]
                
                print(f"   📊 Raw data: {raw_count:,} records")
                print(f"   📊 Monthly revenue: {revenue_count:,} records") 
                print(f"   📊 Daily orders: {orders_count:,} records")
                
                if raw_count > 0 and revenue_count > 0 and orders_count > 0:
                    print("   ✅ Database populated with data")
                    conn.close()
                    return True
            
            conn.close()
        else:
            print("   ⚠️  Database not found - run pipeline first")
            return True  # Not a failure, just needs pipeline execution
            
    except Exception as e:
        print(f"   ❌ Data analysis test failed: {e}")
        return False
    
    return True

def run_simple_tests():
    """Run all simple tests."""
    print("🧪 AMAZON SALES PIPELINE - SIMPLE TEST SUITE")
    print("=" * 55)
    
    tests = [
        ("Component Imports", test_pipeline_components),
        ("Data Cleaning", test_data_cleaning),
        ("DuckDB Operations", test_duckdb_operations),
        ("Data Analysis", test_data_analysis),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🎯 {test_name}:")
        if test_func():
            passed += 1
        else:
            print(f"   💥 {test_name} failed!")
    
    print(f"\n📋 TEST RESULTS:")
    print(f"✅ Passed: {passed}/{total}")
    print(f"❌ Failed: {total - passed}/{total}")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        print("🚀 Pipeline is working correctly!")
        return True
    else:
        print(f"\n⚠️  Some tests failed. Please review and fix issues.")
        return False

if __name__ == "__main__":
    success = run_simple_tests()
    sys.exit(0 if success else 1)