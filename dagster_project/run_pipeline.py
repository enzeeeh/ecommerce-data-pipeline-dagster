#!/usr/bin/env python3
"""
Test script to execute the Amazon Sales Pipeline
"""

import sys
import os
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Now import the pipeline components
try:
    from jobs.data_pipeline import amazon_sales_pipeline
    from resources.duckdb_resource import duckdb_resource
    
    print("🚀 Starting Amazon Sales Pipeline Execution...")
    print("=" * 50)
    
    # Execute the pipeline
    job_result = amazon_sales_pipeline.execute_in_process(
        resources={'duckdb_resource': duckdb_resource}
    )
    
    if job_result.success:
        print("\n✅ PIPELINE EXECUTION SUCCESSFUL!")
        print(f"📊 Job run ID: {job_result.run_id}")
        print(f"🎯 Total ops executed: {len(job_result.all_node_events)}")
        
        # Check if database was created
        db_path = Path("../data/sales.duckdb")
        if db_path.exists():
            print(f"📁 Database created: {db_path}")
            print(f"💾 Database size: {db_path.stat().st_size / 1024 / 1024:.1f} MB")
        
        print("\n🎉 Ready to run analysis notebook!")
        
    else:
        print("\n❌ PIPELINE EXECUTION FAILED!")
        print("Error details:")
        for event in job_result.all_node_events:
            if event.is_failure:
                print(f"  • {event}")
                
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the dagster_project directory")
    
except Exception as e:
    print(f"❌ Execution error: {e}")
    import traceback
    traceback.print_exc()