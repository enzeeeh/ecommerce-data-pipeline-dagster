# Tests Overview

This document explains the purpose of each test file in `dagster_project/tests/`, what the tests validate, and how to run them.

## Test organization

- Unit tests: focus on individual ops and helper functions (`test_data_loading.py`, `test_data_processing.py`, `test_analytics.py`, `test_duckdb_analytics.py`).
- Integration tests: run the complete pipeline end-to-end (`test_integration.py`, `test_pipeline_execution.py`).
- Helpers and runners: convenience scripts for manual checks (`simple_tests.py`, `run_tests.py`).
- Shared fixtures live in `conftest.py` (e.g., `sample_csv_file`, `test_duckdb`).

## Fixtures (in `conftest.py`)

- `sample_csv_file`: creates a temporary CSV containing 3 sample orders used by many unit tests.
- `test_duckdb`: provides a temporary DuckDB database file path for isolated DB tests.
- `sample_dataframe`: small pandas DataFrame used when a DataFrame is needed directly.

---

## File-by-file summary

- `test_data_loading.py`
  - Purpose: unit tests for the `load_csv_data` op.
  - Key checks:
    - Successful CSV discovery and validation (returns a readable CSV path).
    - Proper error raising when file is missing or contains invalid content.
  - Fixtures used: `sample_csv_file`.

- `test_data_processing.py`
  - Purpose: unit and small-integration tests for data cleaning and insertion ops.
  - Key checks:
    - `clean_amazon_sales_data` correctly applies business rules (cancelled orders set to amount 0, currency defaults, date parsing).
    - `insert_raw_data_to_duckdb` inserts cleaned records into a DuckDB test database and returns the inserted count.
    - End-to-end small-pipeline integration using temp DB and cleaned CSV.
  - Fixtures used: `sample_csv_file`, `test_duckdb`.

- `test_analytics.py`
  - Purpose: unit tests for analytics ops (`create_monthly_revenue_table`, `create_daily_orders_table`).
  - Key checks:
    - Analytics SQL produces expected aggregations for a constructed sample dataset.
    - Both ops work together and their aggregates are consistent.
    - Ops handle empty input gracefully (return `0` inserted/created rows).
  - Fixtures used: `test_duckdb` (via helper setup function inside the test file).

- `test_duckdb_analytics.py`
  - Purpose: quick smoke tests that the analytics tables exist and are non-empty in the project's `data/sales.duckdb` file.
  - Key checks:
    - `monthly_revenue` and `daily_orders` tables exist.
    - Both tables contain at least one row.
  - Notes: This file expects a populated `data/sales.duckdb` in the repository. Run the pipeline first if tables are missing.

- `test_pipeline_execution.py`
  - Purpose: verify the entire Dagster job `amazon_sales_pipeline` executes and produces expected DB tables and columns.
  - Key checks:
    - Job runs successfully and all ops report success.
    - Database file exists and expected tables are created (`raw_amazon_sales`, `monthly_revenue`, `daily_orders`).
    - Basic schema checks and sample record assertions (required columns, non-empty tables).
  - Notes: This test executes the real pipeline (may write to `data/sales.duckdb`). Use a temporary DB or run in an isolated environment when possible.

- `test_integration.py`
  - Purpose: full integration tests that run the pipeline in a temporary directory and verify files/tables created by the run.
  - Key checks:
    - Pipeline completes end-to-end with temporary data and DB.
    - Tables are created and contain data.
    - Data quality handling (e.g., cancelled orders setting) is applied.
    - Failure scenarios (missing CSV) are handled gracefully by the job.
  - Fixtures used: `sample_csv_file` (sometimes the test writes its own test CSV content).

- `simple_tests.py`
  - Purpose: lightweight manual runner (not a pytest test) that performs quick checks for imports, data cleaning logic, DuckDB operations, and simple analysis steps.
  - Use-case: fast sanity checks during development; run directly as a script.

- `run_tests.py`
  - Purpose: convenience test runner around `pytest` to run all tests or specific suites.
  - Usage:
    - Run all tests: `python run_tests.py`
    - Run a specific suite: `python run_tests.py loading` (available suites: `loading`, `processing`, `analytics`, `integration`).

---

## How to run the tests

1. Activate your project's virtual environment and ensure dev dependencies are installed (pytest, duckdb, etc.):

```powershell
conda activate dagster-pipeline  # or your venv
pip install -r requirements-dev.txt
```

2. Run all tests with pytest (from repository root):

```powershell
pytest dagster_project/tests -v -s
```

3. Or use the bundled runner from the tests folder:

```powershell
python dagster_project/tests/run_tests.py
# or run a specific suite
python dagster_project/tests/run_tests.py analytics
```

## Notes & best practices

- Use temporary DBs for unit tests (`test_duckdb` fixture) so you don't overwrite your development `data/sales.duckdb`.
- Integration and pipeline execution tests may be slow and write to disk; run them selectively.
- If a test expects `data/sales.duckdb` to exist (see `test_duckdb_analytics.py`), run the pipeline first or adjust the test to use a temp DB.
- Keep `conftest.py` fixtures representative of real data but small enough for fast test runs.

If you want, I can:
- Add a short badge to the README that links to the test runner or CI job.
- Move `run_tests.py` into a `scripts/` directory and update references.
