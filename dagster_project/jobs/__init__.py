"""
Job definitions for the Dagster pipeline.
"""

from dagster_project.jobs.data_pipeline import amazon_sales_pipeline

__all__ = ["amazon_sales_pipeline"]