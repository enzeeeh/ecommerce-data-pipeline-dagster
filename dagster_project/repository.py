"""
Dagster repository for Amazon sales data pipeline.

This is the main entry point for the Dagster project.
Run with: dagster dev
"""

from dagster import repository
from jobs.data_pipeline import amazon_sales_pipeline


@repository
def amazon_sales_repository():
    """
    Dagster repository containing the Amazon sales data pipeline.
    
    This repository includes:
    - amazon_sales_pipeline: Complete data pipeline from CSV to analytics
    
    To run the pipeline:
    1. Navigate to the dagster_project directory
    2. Run: dagster dev
    3. Open the Dagster UI (usually http://localhost:3000)
    4. Execute the amazon_sales_pipeline job
    """
    return [amazon_sales_pipeline]