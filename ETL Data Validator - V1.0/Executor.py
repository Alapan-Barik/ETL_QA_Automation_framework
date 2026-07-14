# Databricks notebook source
# MAGIC %md
# MAGIC This is a **plug-and-play** version of a source-to-target
# MAGIC regression harness originally built ETL build.
# MAGIC  
# MAGIC   Everything this file needs to run comes from `validator.py` (loaded below).
# MAGIC   It runs two ways:
# MAGIC   1. **In Databricks** - import both files into a Repo/Workspace folder and
# MAGIC      "Run all". `dbutils`, `spark` and `display()` are provided by the runtime.
# MAGIC   2. **Locally / in CI** - `python executor-generic.py` with `pyspark` installed. it validates a tiny built-in demo dataset so you can see every
# MAGIC      check - and its on-screen Pass/Fail + mismatching rows - working end to end.

# COMMAND ----------

# DBTITLE 1,Load Data_Validator
# MAGIC %run "/Users/alapanbarik26@gmail.com/ETL Validator/Data_Validator"

# COMMAND ----------

# DBTITLE 1,Configuration -- EDIT ME
from datetime import datetime

data = {}           #dictionary that collects the result of every check below
final_json = {}      #optional: fill this in if you want to surface specific attributes separately from the full `data` blob on exit

#--- Global variables ------------------------------------------------------
dbutils.widgets.text("as_of_date", datetime.now().strftime("%Y-%m-%d"))
as_of_date = dbutils.widgets.get("as_of_date")
current_date = datetime.now().strftime("%Y-%m-%d")
dbutils.widgets.text("rundate", current_date)
rundate = dbutils.widgets.get("rundate")

snap_date = as_of_date   # the snapshot / partition date your source & target queries filter on
print(f"********** Validation run for {snap_date} **********")


# COMMAND ----------


# ADF / orchestrator metadata (purely descriptive - wire these up to your real pipeline's job id / run id / trigger time if you have one) --------
job_id = "GENERIC_VALIDATION_JOB"
run_id = "local-run"
triggered_date = as_of_date

#  Where the report files land. On Databricks point this at DBFS or a Unity
#  Catalog volume, e.g. f"/dbfs/FileStore/validation_reports/{job_id}_{snap_date}".
REPORT_DIR = f"./validation_reports/{job_id}_{snap_date}"
REPORT_TITLE = "Source-to-Target Validation"  # heading on the HTML dashboard report

#  Which report file(s) to write - flip any of these on/off as you like.
EXPORT_JSON = True
EXPORT_CSV = True    # off by default; flip to True any time you want a CSV alongside the rest
EXPORT_HTML = True


#  Delta table to append the run's report to. Set to None to skip that write
#  entirely (it's also skipped automatically when Delta isn't available, e.g.
#  a plain local Spark session without delta-spark installed).
VALIDATION_REPORT_TABLE = None   


# COMMAND ----------

# DBTITLE 1,ADF JOB TRIGGER CHECK

#    ADF JOB TRIGGER CHECK
 
#   Purely descriptive - Wire `job_id` / `run_id` / `triggered_date` above up to your
#   real ADF/Airflow/Databricks Workflows metadata if you have it


adf_status = "Pass"
data["ADF Trigger Check"] = {
    "Status": adf_status,
    "Description": f"Job ID: {job_id}\n Run ID: {run_id}\n Triggered Date: {triggered_date}\n Result: {adf_status}",
}


# COMMAND ----------

# DBTITLE 1,Source Query -- EDIT ME

SOURCE_SQL = None
#  Example:
#  SOURCE_SQL = f"""
#      SELECT customer_id, first_name, last_name, email, country, signup_date
#      FROM source_db.customer
#      WHERE snap_date = '{snap_date}'
#  """

TARGET_SQL = None
#  Example:
#  TARGET_SQL = f"""
#      SELECT customer_id, first_name, last_name, email, country, signup_date
#      FROM curated_db.customer
#      WHERE snap_date = '{snap_date}'
#  """


    # ########Delete this function once you've
    # ########set SOURCE_SQL / TARGET_SQL to your real queries.

def _build_demo_source_and_target():
    """Tiny built-in dataset so this file produces real Pass/Fail output the
    moment you run it. It deliberately seeds a few realistic ETL bugs in target - 

    #    - customer 103 was loaded twice                (composite key / duplicate check)
    #    - customer 102's last name drifted in transform (comparison check)
    #    - customer 104's email was dropped              (mandatory column check)

    """
    columns = ["customer_id", "first_name", "last_name", "email", "country", "signup_date"]

    source_rows = [
        (101, "Alice",  "Johnson", "alice.johnson@example.com", "US", "2025-01-10"),
        (102, "Bob",    "Smith",   "bob.smith@example.com",     "US", "2025-01-11"),
        (103, "Carla",  "Diaz",    "carla.diaz@example.com",    "MX", "2025-01-12"),
        (104, "Dinesh", "Kumar",   "dinesh.kumar@example.com",  "IN", "2025-01-13"),
        (105, "Elin",   "Berg",    "elin.berg@example.com",     "SE", "2025-01-14"),
    ]

    target_rows = [
        (101, "Alice",  "Johnson", "alice.johnson@example.com", "US", "2025-01-10"),
        (102, "Bob",    "Smyth",   "bob.smith@example.com",     "US", "2025-01-11"),
        (103, "Carla",  "Diaz",    "carla.diaz@example.com",    "MX", "2025-01-12"),
        (103, "Carla",  "Diaz",    "carla.diaz@example.com",    "MX", "2025-01-12"),
        (104, "Dinesh", "Kumar",   "",                          "IN", "2025-01-13"),
        (105, "Elin",   "Berg",    "elin.berg@example.com",     "SE", "2025-01-14"),
    ]

    src_df = spark.createDataFrame(source_rows, columns)
    tgt_df = spark.createDataFrame(target_rows, columns)

    key_col = KEY_COLUMNS[0]
    src_df.withColumn("keyColumn", src_df[key_col].cast("string")).createOrReplaceTempView("source")
    tgt_df.withColumn("keyColumn", tgt_df[key_col].cast("string")).createOrReplaceTempView("target")


# COMMAND ----------

# DBTITLE 1,Check parameters
#  --- Check parameters -------------------------------------------------------

KEY_COLUMNS = ["customer_id"]

#Columns that must never be null/blank in `target`.
MANDATORY_COLUMNS = ["first_name", "last_name", "email"]

#Columns to compare row-by-row between `source` and `target` (same name on both sides here; comparison_check() also accepts two different lists if your source and target column names diverge).
COMPARE_COLUMNS = ["first_name", "last_name", "email", "country", "signup_date"]

EXPECTED_SCHEMA = {
    "customer_id": "bigint",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "country": "string",
    "signup_date": "string",
}

# COMMAND ----------

if SOURCE_SQL and TARGET_SQL:
    #  A single key column is used as-is; multiple key columns are concatenated
    #  into one synthetic "keyColumn" so comparison_check() has a single column
    #  to join source to target
    key_expr = KEY_COLUMNS[0] if len(KEY_COLUMNS) == 1 else "CONCAT(" + ", ".join(KEY_COLUMNS) + ")"

    spark.sql(SOURCE_SQL).createOrReplaceTempView("_source_raw")
    spark.sql(f"SELECT {key_expr} AS keyColumn, * FROM _source_raw").createOrReplaceTempView("source")

    spark.sql(TARGET_SQL).createOrReplaceTempView("_target_raw")
    spark.sql(f"SELECT {key_expr} AS keyColumn, * FROM _target_raw").createOrReplaceTempView("target")
else:
    print("SOURCE_SQL / TARGET_SQL are not set - validating the built-in demo dataset instead.\n"
          "Set both above (or register your own 'source' / 'target' temp views) to validate real data.")
    _build_demo_source_and_target()

# COMMAND ----------

# MAGIC %md
# MAGIC    Basic Checks
# MAGIC   - Source and Target Count Check
# MAGIC   - Composite Key Check
# MAGIC   - Duplicate Row Check
# MAGIC   - Mandatory Column Check
# MAGIC   - Comparison Check
# MAGIC   - Table Schema Validation
# MAGIC
# MAGIC   Each check prints its own Pass/Fail result - a small Source/Target Count
# MAGIC   table for the count check, an explicit "Mismatch count: N" line for every
# MAGIC   other check, and (for anything that failed) the actual mismatching/
# MAGIC   duplicate/null rows - straight to the screen as it runs, then stores its
# MAGIC   result dict under `data`.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,COUNT CHECK
print("Source - Target Count Check -->\n")
result = SourceCountCheck("source", "target")
data["Source Vs Target Count Check"] = result

# COMMAND ----------

# DBTITLE 1,Composite Key Check
print("Composite Key Check --> \n")
result = composite_key_check("target", KEY_COLUMNS)
data["Composite Key Check"] = result


# COMMAND ----------

# DBTITLE 1,Duplicate Row Check
try:
    print("Duplicate Row Check --> \n")
    result = DuplicateRowCheck("target", KEY_COLUMNS)
    data["Duplicate Row Check"] = result
except Exception as e:
    print("DuplicateRowCheck function failed\n" + str(e))

# COMMAND ----------

# DBTITLE 1,Mandatory Column Check
try:
    print("Mandatory Column Check --> \n")
    result = MandatoryColumnCheck("target", MANDATORY_COLUMNS)
    data["Mandatory Column Check"] = result
except Exception as e:
    print("MandatoryColumnCheck function failed\n" + str(e))

# COMMAND ----------

# DBTITLE 1,Comparison Check
#Comparison Check - one column at a time

if "data" not in globals():
    data = {}

try:
    column_results = []
    for col_name in COMPARE_COLUMNS:
        print(f"Comparison Check --> {col_name}")
        column_results.append(comparison_check("source", "target", col_name, col_name, "keyColumn"))
        print()

    overall_status = "Fail" if any(r["Status"] == "Fail" for r in column_results) else "Pass"
    combined_rows = []
    for r in column_results:
        combined_rows.extend(r.get("mismatched_rows", []))

    result = {
        "Status": overall_status,
        "Description": "\n".join(r["Description"] for r in column_results),
        "records_tested": column_results[0]["records_tested"] if column_results else 0,
        "mismatch_count": sum(r["mismatch_count"] for r in column_results),
        "mismatched_rows": combined_rows[:20],
    }
    data["Comparison Check"] = result
except Exception as e:
    print("ComparisonCheck function failed\n" + str(e))

# COMMAND ----------

# DBTITLE 1,Table Schema Validation
try:
    print("Table Schema Validation --> \n")
    result = table_schema_validation("target", EXPECTED_SCHEMA)
    data["Table Schema Validation"] = result
except Exception as e:
    print("table_schema_validation function failed\n" + str(e))

# COMMAND ----------

# DBTITLE 1,PASS/FAIL SUMMARY
#On-screen Pass/Fail summary of everything above
print_summary(data)

# COMMAND ----------

# DBTITLE 1,Export the test report

#   Write the collected results to any file format. `export_report` handles
#   json / csv / txt / md / html / xlsx on the driver; `report_to_spark_dataframe`
#   gives you a Spark DataFrame you can `.write` to delta / parquet / csv anywhere.

#  Driver-side files - toggle EXPORT_JSON / EXPORT_CSV / EXPORT_HTML / EXPORT_XLSX

if EXPORT_JSON:
    export_report(data, f"{REPORT_DIR}.json")
if EXPORT_CSV:
    export_report(data, f"{REPORT_DIR}.csv")
if EXPORT_HTML:
    export_report(data, f"{REPORT_DIR}.html", title=REPORT_TITLE)

print(f"Reports written under {REPORT_DIR}.*")

# COMMAND ----------

# DBTITLE 1,Exit
#Exit: hand the collected results back as JSON, e.g. for a parent job / ADF pipeline to read.
import json
json_data = json.dumps(data, default=str)
json_result = json.dumps(final_json)
final_result = f"{json_data} ||| {json_result}"
dbutils.notebook.exit(final_result)