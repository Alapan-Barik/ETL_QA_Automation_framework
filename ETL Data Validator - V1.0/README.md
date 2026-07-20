# ETL QA Automation Framework

A reusable, plug-and-play **source-to-target data validation harness** for Spark / Databricks ETL pipelines. Point it at a source and target table (or view), and it runs a full suite of reconciliation and data-quality checks — printing Pass/Fail results, mismatching rows, and exportable reports (JSON, CSV, HTML dashboard, Markdown, XLSX).

Built to plug into any existing ETL job with minimal wiring: set two SQL queries, list your key/mandatory/comparison columns, and run.

Three executors ship on top of the same check library, for the three source/target shapes a real pipeline actually hits:

| Executor | Source | Target |
|---|---|---|
| `Executor.py` | Table / view (SQL) | Table / view (SQL) |
| `File_To_Table_Executor.py` | File (CSV, TXT, JSON, pipe/tab-delimited, fixed-width) | Table / view (SQL) |
| `Table_To_File_Executor.py` | Table / view (SQL) | File (CSV, TXT, JSON, pipe/tab-delimited, fixed-width) |

All three call the exact same checks in `Data_Validator.py` - the file-based executors only differ in how the `source`/`target` temp view gets built.

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
├── Data_Validator.py            # Core validation library — all checks + reporting utilities (unmodified by the file executors)
├── File_Reader.py                # Reusable, configurable file parser — file(s) -> clean Spark DataFrame
├── Executor.py                   # Orchestrator notebook — table -> table, configure & run checks, export reports
├── File_To_Table_Executor.py     # Orchestrator notebook — file -> table
├── Table_To_File_Executor.py     # Orchestrator notebook — table -> file
├── sample_files/                 # Demo input files used by the file-based executors when no config is set
└── validation_reports/           # Generated Pass/Fail reports (JSON, CSV, HTML dashboard)
```

- **`Data_Validator.py`** is Spark-native — it just uses whichever `SparkSession` is already active, so the same code works in a Databricks notebook, a scheduled job, or a plain `spark-submit` script. No Databricks-specific imports. None of the three executors modify it; they only ever call its functions.
- **`File_Reader.py`** is the reusable file parser behind the two file-based executors: `FileReader.read_file(config) -> DataFrame`. It auto-detects CSV, TXT, JSON, pipe/tab-delimited and fixed-width files (or takes an explicit `file_type`), and standardises the result — column renaming/mapping, duplicate-column handling, null normalisation, trimming, dtype casting — so the DataFrame it returns is shaped exactly like a table would be. See the module docstring for the full config reference.
- **`Executor.py` / `File_To_Table_Executor.py` / `Table_To_File_Executor.py`** are the three orchestration layers. Each one only differs in how it builds the `source` and `target` temp views (SQL query vs. `File_Reader.read_file()`); every check call after that point is identical.

## How to run

**In Databricks**
1. Import all files into a Repo or Workspace folder.
2. Open `Executor.py` (table → table), `File_To_Table_Executor.py` (file → table), or `Table_To_File_Executor.py` (table → file) and click **Run all**. `dbutils`, `spark`, and `display()` are provided by the Databricks runtime.

**Locally / in CI**
1. Install `pyspark`.
2. Run one of the three executors as a plain Python script, e.g. `python File_To_Table_Executor.py`.
3. With no source/target configured, each one validates a small built-in demo dataset (5–6 sample customer records with a few seeded ETL bugs — a duplicate row, a drifted column value, a dropped mandatory field) so you can see every check, and its on-screen Pass/Fail output, working end to end. `File_To_Table_Executor.py` and `Table_To_File_Executor.py` read their demo data from `sample_files/customers_source.csv` / `sample_files/customers_target.csv` respectively, so the demo also exercises `File_Reader.py`.

## Configuring for a real pipeline

**Table → Table** (`Executor.py`) — everything you're likely to change lives near the top of the file:

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

**File → Table** (`File_To_Table_Executor.py`) — same `KEY_COLUMNS` / `MANDATORY_COLUMNS` / `COMPARE_COLUMNS` / `EXPECTED_SCHEMA` block, but `SOURCE_SQL` is replaced with `SOURCE_FILE_CONFIG`, a dict consumed by `File_Reader.read_file()`:

```python
SOURCE_FILE_CONFIG = {
    "file_path": "/dbfs/FileStore/extracts/customer_extract.csv",
    "label": "source",
    "file_type": "csv",
    "trim_spaces": True,
    "null_values": ["NA", "NULL", ""],
}

TARGET_SQL = f"""
    SELECT customer_id, first_name, last_name, email, country, signup_date
    FROM curated_db.customer
    WHERE snap_date = '{snap_date}'
"""
```

An irregular vendor layout — pipe-delimited, a 2-line metadata banner, a 1-line footer, no header row of its own:

```python
SOURCE_FILE_CONFIG = {
    "file_path": "/dbfs/FileStore/extracts/customer_extract.txt",
    "label": "source",
    "file_type": "pipe",
    "header": False,
    "skip_rows": 2,
    "footer_rows": 1,
    "column_names": ["customer_id", "first_name", "last_name", "email", "country", "signup_date"],
}
```

**Table → File** (`Table_To_File_Executor.py`) — the mirror image: `SOURCE_SQL` stays a query, `TARGET_FILE_CONFIG` replaces `TARGET_SQL`:

```python
SOURCE_SQL = f"""
    SELECT customer_id, first_name, last_name, email, country, signup_date
    FROM source_db.customer
    WHERE snap_date = '{snap_date}'
"""

TARGET_FILE_CONFIG = {
    "file_path": "/dbfs/FileStore/exports/customer_export.csv",
    "label": "target",
    "file_type": "csv",
    "trim_spaces": True,
    "null_values": ["NA", "NULL", ""],
}
```

### `File_Reader.py` config reference

Every key besides `file_path` is optional:

| Key | Purpose |
|---|---|
| `file_path` | Path to the file (required) |
| `file_type` | `csv` \| `txt` \| `json` \| `pipe` \| `tab` \| `fixed_width` — auto-detected from the extension if omitted |
| `delimiter` | Field separator for csv/txt (default `,`) |
| `encoding` | File encoding (default `utf-8`) |
| `quote_char` / `escape_char` | CSV quoting/escaping |
| `header` | `False` when the file has no header row |
| `skip_rows` / `footer_rows` | Metadata/banner lines to drop from the top/bottom |
| `comment_prefix` | Lines starting with this string are dropped, e.g. `"#"` |
| `column_names` | Explicit column list — required when `header=False`, or to rename `_c0.._cN` |
| `column_mapping` | `{"raw_name": "clean_name"}` |
| `dtype_mapping` | `{"column": "int" / "double" / "string" / "date" / ...}` (any Spark cast-able type) |
| `null_values` | Extra string tokens (`["NA", "NULL"]`) to treat as real nulls |
| `trim_spaces` | Strip whitespace on every string column (default `True`) |
| `colspecs` | Fixed-width only: `[("name", start, length), ...]`, 0-indexed |
| `json_lines` / `multiline_json` | JSON Lines (default) vs. a single multi-line JSON array/object |

`File_Reader.py` raises a clear `FileNotFoundError` / `ValueError` / `RuntimeError` for a missing file, an empty file, malformed JSON, or a `column_mapping`/`dtype_mapping` that references a column the file doesn't have — the same "fail loudly with a specific message" style `Data_Validator.py` already uses.

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
