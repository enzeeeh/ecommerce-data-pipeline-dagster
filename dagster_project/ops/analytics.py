"""
Analytics ops for creating business intelligence tables.
"""

from dagster import op, get_dagster_logger, In, Out


@op(
    name="create_monthly_revenue_table",
    description="Create monthly revenue by category analytical table",
    ins={"records_inserted": In(int, description="Number of records in raw table")},
    out=Out(int, description="Number of monthly revenue records created"),
    required_resource_keys={"duckdb_resource"}
)
def create_monthly_revenue_table(context, records_inserted: int) -> int:
    """
    Create monthly revenue by category analytical table.
    
    Args:
        records_inserted: Number of records in raw table
        
    Returns:
        int: Number of monthly revenue records created
    """
    logger = get_dagster_logger()
    logger.info("📊 Creating monthly revenue by category table...")
    
    # Get DuckDB resource
    duckdb_conn = context.resources.duckdb_resource
    raw_table = duckdb_conn._tables['raw_data']
    revenue_table = duckdb_conn._tables['monthly_revenue']
    
    # Debug: Check raw data first
    logger.info("🔍 Debugging raw data for monthly revenue...")
    raw_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    logger.info(f"📊 Total raw records: {raw_count}")
    
    # Check data quality for revenue analysis
    quality_check = duckdb_conn.execute(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN status != 'Cancelled' THEN 1 END) as non_cancelled,
            COUNT(CASE WHEN amount > 0 THEN 1 END) as positive_amount,
            COUNT(CASE WHEN status != 'Cancelled' AND amount > 0 THEN 1 END) as valid_revenue,
            COUNT(DISTINCT category) as unique_categories,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount
        FROM {raw_table}
        WHERE date_col IS NOT NULL
    """).fetchone()
    
    logger.info(f"💰 Revenue analysis data quality:")
    logger.info(f"  • Total records: {quality_check[0]}")
    logger.info(f"  • Non-cancelled: {quality_check[1]}")
    logger.info(f"  • Positive amounts: {quality_check[2]}")
    logger.info(f"  • Valid for revenue: {quality_check[3]}")
    logger.info(f"  • Unique categories: {quality_check[4]}")
    logger.info(f"  • Amount range: ${quality_check[5]:.2f} to ${quality_check[6]:,.2f}")
    
    # Create monthly revenue analysis
    monthly_revenue_query = f"""
    INSERT INTO {revenue_table} (year_month, category, total_revenue, order_count, avg_order_value)
    SELECT 
        strftime('%Y-%m', date_col) as year_month,
        category,
        SUM(COALESCE(amount, 0)) as total_revenue,
        COUNT(*) as order_count,
        AVG(COALESCE(amount, 0)) as avg_order_value
    FROM {raw_table}
    WHERE date_col IS NOT NULL 
      AND category IS NOT NULL
      AND (status IS NULL OR status != 'Cancelled') 
      AND amount > 0
    GROUP BY strftime('%Y-%m', date_col), category
    ORDER BY year_month, total_revenue DESC
    """
    
    # Clear existing data and insert new
    duckdb_conn.execute(f"DELETE FROM {revenue_table}")
    duckdb_conn.execute(monthly_revenue_query)
    
    # Get count of created records
    count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {revenue_table}").fetchone()
    revenue_records = count_result[0]
    
    logger.info(f"✅ Created {revenue_records} monthly revenue records")
    
    if revenue_records > 0:
        # Log sample data
        sample_data = duckdb_conn.execute(f"""
            SELECT year_month, category, total_revenue, order_count
            FROM {revenue_table}
            ORDER BY total_revenue DESC
            LIMIT 5
        """).fetchall()
        
        logger.info("📈 Top 5 monthly revenue records:")
        for row in sample_data:
            year_month, category, revenue, count = row
            logger.info(f"• {year_month} {category}: ${revenue:,.0f} ({count} orders)")
    else:
        logger.warning("⚠️ No monthly revenue records created - checking data issues...")
    
    return revenue_records


@op(
    name="create_daily_orders_table", 
    description="Create daily orders by status analytical table",
    ins={"records_inserted": In(int, description="Number of records in raw table")},
    out=Out(int, description="Number of daily order records created"),
    required_resource_keys={"duckdb_resource"}
)
def create_daily_orders_table(context, records_inserted: int) -> int:
    """
    Create daily orders by status analytical table.
    
    Args:
        records_inserted: Number of records in raw table
        
    Returns:
        int: Number of daily order records created
    """
    logger = get_dagster_logger()
    logger.info("📊 Creating daily orders by status table...")
    
    # Get DuckDB resource
    duckdb_conn = context.resources.duckdb_resource
    raw_table = duckdb_conn._tables['raw_data']
    orders_table = duckdb_conn._tables['daily_orders']
    
    # Debug: Check raw data first
    logger.info("🔍 Debugging raw data for daily orders...")
    raw_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    logger.info(f"📊 Total raw records: {raw_count}")
    
    # Check unique dates and statuses
    date_check = duckdb_conn.execute(f"""
        SELECT COUNT(DISTINCT date_col) as unique_dates,
               COUNT(DISTINCT status) as unique_statuses,
               MIN(date_col) as min_date,
               MAX(date_col) as max_date
        FROM {raw_table}
        WHERE date_col IS NOT NULL
    """).fetchone()
    logger.info(f"📅 Date range: {date_check[2]} to {date_check[3]} ({date_check[0]} unique dates)")
    logger.info(f"📋 Unique statuses: {date_check[1]}")
    
    # Show sample statuses
    status_sample = duckdb_conn.execute(f"""
        SELECT status, COUNT(*) as count
        FROM {raw_table}
        GROUP BY status
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    logger.info("📊 Status distribution:")
    for status, count in status_sample:
        logger.info(f"  • {status}: {count} records")
    
    # Create daily orders analysis
    daily_orders_query = f"""
    INSERT INTO {orders_table} (order_date, status, order_count, total_quantity, total_amount)
    SELECT 
        date_col as order_date,
        status,
        COUNT(*) as order_count,
        SUM(COALESCE(qty, 1)) as total_quantity,
        SUM(COALESCE(amount, 0)) as total_amount
    FROM {raw_table}
    WHERE date_col IS NOT NULL 
      AND status IS NOT NULL
    GROUP BY date_col, status
    ORDER BY order_date, status
    """
    
    # Clear existing data and insert new
    duckdb_conn.execute(f"DELETE FROM {orders_table}")
    result = duckdb_conn.execute(daily_orders_query)
    
    # Get count of created records
    count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {orders_table}").fetchone()
    order_records = count_result[0]
    
    logger.info(f"✅ Created {order_records} daily order records")
    
    if order_records > 0:
        # Log sample data by status
        status_summary = duckdb_conn.execute(f"""
            SELECT status, COUNT(*) as days, SUM(order_count) as total_orders, SUM(total_amount) as total_amount
            FROM {orders_table}
            GROUP BY status
            ORDER BY total_orders DESC
        """).fetchall()
        
        logger.info("📈 Daily orders summary by status:")
        for row in status_summary:
            status, days, orders, amount = row
            amount_str = f"${amount:,.0f}" if amount else "$0"
            logger.info(f"• {status}: {orders} orders across {days} days, {amount_str} total")
    else:
        logger.warning("⚠️ No daily order records created - checking data issues...")
        
        # Additional debugging
        null_check = duckdb_conn.execute(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(date_col) as non_null_dates,
                COUNT(status) as non_null_status
            FROM {raw_table}
        """).fetchone()
        logger.info(f"🔍 Data quality check: {null_check[1]}/{null_check[0]} non-null dates, {null_check[2]}/{null_check[0]} non-null statuses")
    
    return order_records