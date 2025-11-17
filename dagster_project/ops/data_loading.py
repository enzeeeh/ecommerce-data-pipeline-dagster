"""
Data loading ops for CSV processing.
"""

import pandas as pd
from pathlib import Path
from dagster import op, get_dagster_logger, In, Out


@op(
    name="load_csv_data",
    description="Load raw CSV data from file",
    out=Out(str, description="Path to raw CSV file")
)
def load_csv_data(context) -> str:
    """
    Load raw Amazon sales CSV data.
    
    Returns:
        str: Path to the CSV file
    """
    logger = get_dagster_logger()
    
    # CSV file configuration - use absolute path
    current_dir = Path(__file__).parent.parent
    csv_file = current_dir / ".." / "data" / "Amazon Sale Report.csv"
    csv_path = Path(csv_file).resolve()
    
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Verify file can be read
    try:
        df_test = pd.read_csv(csv_path, nrows=5)
        logger.info(f"✅ Successfully validated CSV file: {csv_path}")
        logger.info(f"📊 Columns: {len(df_test.columns)}")
        logger.info(f"📁 File size: {csv_path.stat().st_size / (1024 * 1024):.1f} MB")
    except Exception as e:
        raise ValueError(f"Error reading CSV file: {e}")
    
    return str(csv_path)