"""
Test DuckDB analytics tables creation and data integrity
"""
import duckdb
import pytest

DB_PATH = "data/sales.duckdb"

def test_monthly_revenue_table_exists():
    conn = duckdb.connect(DB_PATH)
    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    assert "monthly_revenue" in tables
    conn.close()


def test_daily_orders_table_exists():
    conn = duckdb.connect(DB_PATH)
    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    assert "daily_orders" in tables
    conn.close()


def test_daily_orders_not_empty():
    conn = duckdb.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM daily_orders").fetchone()[0]
    assert count > 0
    conn.close()


def test_monthly_revenue_not_empty():
    conn = duckdb.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM monthly_revenue").fetchone()[0]
    assert count > 0
    conn.close()

# Short comments: test table existence and non-empty analytics tables
