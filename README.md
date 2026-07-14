# ETL QA Automation Framework

A reusable, plug-and-play **source-to-target data validation harness** for Spark / Databricks ETL pipelines. Point it at a source and target table (or view), and it runs a full suite of reconciliation and data-quality checks — printing Pass/Fail results, mismatching rows, and exportable reports (JSON, CSV, HTML dashboard, Markdown, XLSX).

Built to plug into any existing ETL job with minimal wiring: set two SQL queries, list your key/mandatory/comparison columns, and run.

## What it checks

| Check | What it catches |
|---|---|
| `source_target_count_check` | Row counts match between source and target |
| `composite_key_check` | A set of columns still uniquely identifies each row |
| `duplicate_row_check` | No duplicate rows on chosen columns |
| `mandatory_column_check` | Required columns aren't null or blank |
| `comparison_check` | Column values match row-by-row between source and target |
| `schema_validation` | Table columns and types match what's expected |
| `hardcode_column_check` | A column always holds one fixed value |
| `null_check` | A column has no nulls |
| `accepted_values_check` | Values fall within an allowed set |
| `range_check` | Numeric/date values fall within min/max |
| `regex_format_check` | Values match a given pattern |
| `referential_integrity_check` | No orphaned foreign keys between two tables |

Every check returns a plain dictionary (`Status`, `Description`, plus check-specific extras like `mismatch_count`), making results easy to log, aggregate, or write out.

## Project structure

```
ETL Validator/
├── Data_Validator.py     # Core validation library — all checks + reporting utilities
├── Executor.py           # Orchestrator notebook — configure & run checks, export reports
└── validation_reports/   # Generated Pass/Fail reports (JSON, CSV, HTML dashboard)
```

- **`Data_Validator.py`** is Spark-native — it just uses whichever `SparkSession` is already active, so the same code works in a Databricks notebook, a scheduled job, or a plain `spark-submit` script. No Databricks-specific imports.
- **`Executor.py`** is the orchestration layer: it defines source/target queries, which columns to check, and calls into `Data_Validator.py` for each check.

## How to run

**In Databricks**
1. Import both files into a Repo or Workspace folder.
2. Open `Executor.py` and click **Run all**. `dbutils`, `spark`, and `display()` are provided by the Databricks runtime.

**Locally / in CI**
1. Install `pyspark`.
2. Run `Executor.py` as a plain Python script.
3. With no source/target configured, it validates a small built-in demo dataset (5–6 sample customer records with a few seeded ETL bugs — a duplicate row, a drifted column value, a dropped mandatory field) so you can see every check, and its on-screen Pass/Fail output, working end to end.

## Configuring for a real pipeline

Everything you're likely to change for a new pipeline lives in one place, near the top of `Executor.py`:

```python
SOURCE_SQL = f"""
    SELECT customer_id, first_name, last_name, email, country, signup_date
    FROM source_db.customer
    WHERE snap_date = '{snap_date}'
"""

TARGET_SQL = f"""
    SELECT customer_id, first_name, last_name, email, country, signup_date
    FROM curated_db.customer
    WHERE snap_date = '{snap_date}'
"""

KEY_COLUMNS = ["customer_id"]
MANDATORY_COLUMNS = ["first_name", "last_name", "email"]
COMPARE_COLUMNS = ["first_name", "last_name", "email", "country", "signup_date"]
EXPECTED_SCHEMA = {
    "customer_id": "bigint",
    "first_name": "string",
    ...
}
```

Once `SOURCE_SQL` and `TARGET_SQL` are set, the demo dataset is skipped automatically and our real tables are validated instead.

## Reports

Results can be exported in several formats via `export_report(data, path)`:

- **JSON** — full structured results, easy to feed into downstream systems
- **CSV / Markdown** — flat Check/Status/Description table
- **HTML** — a self-contained visual dashboard with pass-rate donut chart, summary metric cards, and per-check mismatch samples
- **XLSX** — spreadsheet output (requires `pandas` + `openpyxl`)

Results can also be converted to a Spark DataFrame via `report_to_spark_dataframe()` and written to Delta/Parquet/CSV for long-term tracking of validation runs.

## Roadmap

This is an early, working version. Planned enhancements include a dedicated CI workflow, sample source/target datasets for a fuller demo, and expanded BFSI-domain checks (reconciliation patterns).

## Author

Built by [Alapan Barik](https://github.com/Alapan-Barik) — QA Engineer (Test Architect) with a background in ETL testing and Data Quality Engineering across the BFSI domain.
