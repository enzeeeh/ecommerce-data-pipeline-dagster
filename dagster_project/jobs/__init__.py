"""
Job definitions for the Dagster pipeline.
"""

from jobs.data_pipeline import amazon_sales_pipeline

__all__ = ["amazon_sales_pipeline"]