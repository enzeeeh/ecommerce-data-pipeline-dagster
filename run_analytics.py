from dagster_project.resources.duckdb_resource import duckdb_resource
from dagster_project.ops.analytics import create_daily_orders_table, create_monthly_revenue_table
import dagster

def main():
    ctx = dagster.build_op_context(resources={'duckdb_resource': duckdb_resource})
    duckdb = ctx.resources.duckdb_resource
    raw_table = duckdb._tables['raw_data']
    raw_count = duckdb.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    print('RAW_COUNT:', raw_count)

    daily_count = create_daily_orders_table(ctx, raw_count)
    print('DAILY_RECORDS_CREATED:', daily_count)
    orders_table = duckdb._tables['daily_orders']
    print('DAILY_SAMPLE:', duckdb.execute(f"SELECT order_date, status, order_count, total_amount FROM {orders_table} ORDER BY total_amount DESC LIMIT 5").fetchall())

    monthly_count = create_monthly_revenue_table(ctx, raw_count)
    print('MONTHLY_RECORDS_CREATED:', monthly_count)
    rev_table = duckdb._tables['monthly_revenue']
    print('REVENUE_SAMPLE:', duckdb.execute(f"SELECT year_month, category, total_revenue, order_count FROM {rev_table} ORDER BY total_revenue DESC LIMIT 5").fetchall())

if __name__ == '__main__':
    main()
