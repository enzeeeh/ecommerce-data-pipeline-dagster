"""
DuckDB resource for database connection and schema management.
"""

import duckdb
from dagster import resource


class DuckDBConnectionWrapper:
    """Wrapper class for DuckDB connection with table configuration"""
    
    def __init__(self, connection, tables, db_file):
        self.connection = connection
        self.tables = tables
        self.db_file = db_file
        self._closed = False
    
    def execute(self, query):
        if self._closed:
            raise RuntimeError("DuckDB connection is already closed")
        return self.connection.execute(query)
    
    def register(self, name, df):
        if self._closed:
            raise RuntimeError("DuckDB connection is already closed")
        return self.connection.register(name, df)
    
    def close(self):
        if not self._closed:
            self.connection.close()
            self._closed = True
    
    def __del__(self):
        """Ensure connection is closed when wrapper is destroyed"""
        self.close()
    
    @property
    def _tables(self):
        return self.tables
    
    @property 
    def _db_file(self):
        return self.db_file


# Singleton connection to ensure only one connection is created
_singleton_connection = None

@resource(
    description="DuckDB connection resource with schema setup"
)
def duckdb_resource(context):
    global _singleton_connection

    if _singleton_connection is not None:
        context.log.info("Reusing existing DuckDB connection.")
        return _singleton_connection

    # Database configuration - use absolute path
    from pathlib import Path
    current_dir = Path(__file__).parent.parent
    db_file = current_dir / ".." / "data" / "sales.duckdb"
    db_file = str(db_file.resolve())

    tables = {
        'raw_data': 'raw_amazon_sales',
        'monthly_revenue': 'monthly_revenue',
        'daily_orders': 'daily_orders'
    }

    # Create connection
    conn = duckdb.connect(db_file)

    # Raw data table schema (optimized for Amazon sales data)
    raw_table_ddl = f"""
    CREATE TABLE IF NOT EXISTS {tables['raw_data']} (
        -- Identifiers
        index_id INTEGER,
        order_id VARCHAR,
        
        -- Date and Time
        date_col DATE,
        
        -- Product Information  
        category VARCHAR,
        size VARCHAR,
        sku VARCHAR,
        asin VARCHAR,
        style VARCHAR,
        
        -- Order Details
        status VARCHAR,
        courier_status VARCHAR,
        qty INTEGER,
        amount DECIMAL(10,2),
        currency VARCHAR(10),
        
        -- Customer Information
        ship_service_level VARCHAR,
        ship_city VARCHAR,
        ship_state VARCHAR,
        ship_postal_code INTEGER,
        ship_country VARCHAR,
        
        -- Sales Channel
        sales_channel VARCHAR,
        fulfilled_by VARCHAR,
        promotion_ids VARCHAR,
        
        -- Data Quality
        data_quality_flag VARCHAR,
        
        -- Metadata
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # Monthly revenue by category analytical table
    monthly_revenue_ddl = f"""
    CREATE TABLE IF NOT EXISTS {tables['monthly_revenue']} (
        year_month VARCHAR,
        category VARCHAR,
        total_revenue DECIMAL(12,2),
        order_count INTEGER,
        avg_order_value DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # Daily orders by status analytical table  
    daily_orders_ddl = f"""
    CREATE TABLE IF NOT EXISTS {tables['daily_orders']} (
        order_date DATE,
        status VARCHAR,
        order_count INTEGER,
        total_quantity INTEGER,
        total_amount DECIMAL(12,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # Execute schema creation
    conn.execute(raw_table_ddl)
    conn.execute(monthly_revenue_ddl)
    conn.execute(daily_orders_ddl)

    # Create wrapper with configuration
    wrapped_conn = DuckDBConnectionWrapper(conn, tables, db_file)

    context.log.info(f"🦆 DuckDB connection established: {db_file}")
    context.log.info(f"📊 Tables configured: {list(tables.values())}")
    context.log.info("🔄 Connection will remain open for parallel analytics ops")

    # Store the singleton connection
    _singleton_connection = wrapped_conn

    return wrapped_conn