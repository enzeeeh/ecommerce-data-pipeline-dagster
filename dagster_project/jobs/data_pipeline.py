"""
Main data pipeline job that orchestrates all ops.
"""

from dagster import job
from dagster_project.ops.data_loading import load_csv_data
from dagster_project.ops.data_processing import clean_amazon_sales_data, insert_raw_data_to_duckdb
from dagster_project.ops.analytics import create_monthly_revenue_table, create_daily_orders_table
from dagster_project.resources.duckdb_resource import duckdb_resource


@job(
    name="amazon_sales_pipeline",
    description="Complete Amazon sales data pipeline from CSV to analytics tables",
    resource_defs={"duckdb_resource": duckdb_resource}
)
def amazon_sales_pipeline():
    """
    Complete data pipeline that processes Amazon sales data:
    
    1. Load raw CSV data
    2. Clean data with business rules  
    3. Insert cleaned data into DuckDB raw table
    4. Create daily orders analytical table
    5. Create monthly revenue analytical table
    
    Dependencies are automatically managed by Dagster based on op inputs/outputs.
    """
    # Load raw CSV
    raw_csv_path = load_csv_data()
    
    # Clean the data
    cleaned_csv_path = clean_amazon_sales_data(raw_csv_path)
    
    # Insert into DuckDB raw table
    records_inserted = insert_raw_data_to_duckdb(cleaned_csv_path)
    
    # Create daily orders table first, then monthly revenue table (sequential)
    daily_order_count = create_daily_orders_table(records_inserted)
    monthly_revenue_count = create_monthly_revenue_table(daily_order_count)

    # Log results for verification
    from dagster import get_dagster_logger
    logger = get_dagster_logger()
    logger.info(f"✅ Daily orders table created with {daily_order_count} records.")
    logger.info(f"✅ Monthly revenue table created with {monthly_revenue_count} records.")