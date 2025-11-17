"""
Data processing ops for cleaning and DuckDB insertion.
"""

import pandas as pd
from pathlib import Path
from dagster import op, get_dagster_logger, In, Out


@op(
    name="clean_amazon_sales_data",
    description="Clean Amazon sales data with business rules",
    ins={"raw_csv_path": In(str, description="Path to raw CSV file")},
    out=Out(str, description="Path to cleaned CSV file")
)
def clean_amazon_sales_data(context, raw_csv_path: str) -> str:
    """
    Clean Amazon sales data based on business rules.
    
    Business Rules:
    - Cancelled orders with missing Amount → Set Amount = 0
    - Missing currency → Set to 'INR' (default)
    - Flag data quality issues for non-cancelled orders with missing Amount
    - Save cleaned data as separate CSV file
    
    Args:
        raw_csv_path: Path to raw CSV file
        
    Returns:
        str: Path to cleaned CSV file
    """
    logger = get_dagster_logger()
    logger.info("🧹 Starting data cleaning pipeline...")
    
    # Load raw data
    df_raw = pd.read_csv(raw_csv_path)
    logger.info(f"📥 Loaded {len(df_raw):,} records from {raw_csv_path}")
    
    df_clean = df_raw.copy()
    
    # Business columns configuration
    amount_col = 'Amount'
    status_col = 'Status'
    currency_col = 'currency'
    date_col = 'Date'
    
    # Count original missing values
    original_amount_nulls = df_clean[amount_col].isna().sum()
    original_currency_nulls = df_clean[currency_col].isna().sum()
    
    # Rule 1: Set Amount = 0 for cancelled orders with missing Amount
    cancelled_missing_amount = (df_clean[status_col] == 'Cancelled') & (df_clean[amount_col].isna())
    cancelled_count = cancelled_missing_amount.sum()
    df_clean.loc[cancelled_missing_amount, amount_col] = 0.0
    
    # Rule 2: Flag non-cancelled orders with missing Amount (data quality issue)
    non_cancelled_missing = (df_clean[status_col] != 'Cancelled') & (df_clean[amount_col].isna())
    flagged_count = non_cancelled_missing.sum()
    if flagged_count > 0:
        df_clean.loc[non_cancelled_missing, 'data_quality_flag'] = 'missing_amount_non_cancelled'
    
    # Rule 3: Set default currency for missing values
    currency_missing = df_clean[currency_col].isna()
    currency_count = currency_missing.sum()
    df_clean.loc[currency_missing, currency_col] = 'INR'
    
    # Rule 4: Convert date column to proper datetime format
    df_clean[date_col] = pd.to_datetime(df_clean[date_col], format='%m-%d-%y')
    
    # Save cleaned data to new CSV file
    cleaned_csv_path = raw_csv_path.replace('.csv', '_cleaned.csv')
    df_clean.to_csv(cleaned_csv_path, index=False)
    
    # Log cleaning statistics
    final_amount_nulls = df_clean[amount_col].isna().sum()
    final_currency_nulls = df_clean[currency_col].isna().sum()
    
    logger.info(f"✅ Cleaned {cancelled_count} cancelled orders (Amount → 0)")
    logger.info(f"✅ Set default currency for {currency_count} records")
    logger.info(f"⚠️  Flagged {flagged_count} non-cancelled orders with missing Amount")
    logger.info(f"📊 Final Amount nulls: {final_amount_nulls}")
    logger.info(f"💾 Saved cleaned data to: {cleaned_csv_path}")
    
    return cleaned_csv_path


@op(
    name="insert_raw_data_to_duckdb",
    description="Insert cleaned CSV data into DuckDB raw table",
    ins={"cleaned_csv_path": In(str, description="Path to cleaned CSV file")},
    out=Out(int, description="Number of records inserted"),
    required_resource_keys={"duckdb_resource"}
)
def insert_raw_data_to_duckdb(context, cleaned_csv_path: str) -> int:
    """
    Insert cleaned data into DuckDB raw table with proper column mapping.
    
    Args:
        cleaned_csv_path: Path to cleaned CSV file
        
    Returns:
        int: Number of records inserted
    """
    logger = get_dagster_logger()
    logger.info(f"💾 Loading cleaned data from: {cleaned_csv_path}")
    
    # Get DuckDB resource
    duckdb_conn = context.resources.duckdb_resource
    
    # Read cleaned data
    df_clean = pd.read_csv(cleaned_csv_path)
    logger.info(f"📥 Loaded {len(df_clean):,} cleaned records")
    
    # Prepare data for DuckDB insertion with column mapping
    df_db = df_clean.copy()
    
    # Column mapping to match DuckDB schema
    column_mapping = {
        'Order ID': 'order_id', 
        'Date': 'date_col',
        'Status': 'status',
        'Fulfilment': 'fulfilled_by',
        'Sales Channel ': 'sales_channel',
        'ship-service-level': 'ship_service_level',
        'Style': 'style',
        'SKU': 'sku',
        'Category': 'category',
        'Size': 'size',
        'ASIN': 'asin',
        'Courier Status': 'courier_status',
        'Qty': 'qty',
        'currency': 'currency',
        'Amount': 'amount',
        'ship-city': 'ship_city',
        'ship-state': 'ship_state',
        'ship-postal-code': 'ship_postal_code',
        'ship-country': 'ship_country',
        'promotion-ids': 'promotion_ids'
    }
    
    # Rename columns that exist in the DataFrame
    existing_renames = {old: new for old, new in column_mapping.items() if old in df_db.columns}
    df_db = df_db.rename(columns=existing_renames)
    
    # Add index column if not present
    if 'index_id' not in df_db.columns:
        df_db['index_id'] = range(len(df_db))
    
    # Select columns that exist in DuckDB schema
    db_columns = ['index_id', 'order_id', 'date_col', 'category', 'size', 'sku', 'asin', 'style',
                  'status', 'courier_status', 'qty', 'amount', 'currency', 'ship_service_level',
                  'ship_city', 'ship_state', 'ship_postal_code', 'ship_country', 'sales_channel',
                  'fulfilled_by', 'promotion_ids', 'data_quality_flag']
    
    available_columns = [col for col in db_columns if col in df_db.columns]
    df_final = df_db[available_columns].copy()
    
    logger.info(f"📋 Prepared {len(df_final)} records with columns: {available_columns}")
    
    # Insert data into DuckDB using bulk insert
    duckdb_conn.register('df_temp', df_final)
    column_list = ', '.join(available_columns)
    table_name = duckdb_conn._tables['raw_data']
    
    insert_query = f"INSERT INTO {table_name} ({column_list}) SELECT * FROM df_temp"
    duckdb_conn.execute(insert_query)
    
    # Verify insertion
    count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    records_inserted = count_result[0]
    
    logger.info(f"✅ Successfully inserted {records_inserted:,} records into {table_name}")
    
    return records_inserted