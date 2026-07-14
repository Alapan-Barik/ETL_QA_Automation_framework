# Databricks notebook source
"""
validator.py
============

Reusable data-validation checks for Spark / Databricks pipelines.

An ETL job reads data from a source, transforms it, and writes it to a
target - a table or a temp view. This module holds the checks that confirm
the target still matches the source: same row count, no duplicates, no
missing values, correct schema, no orphaned foreign keys, and so on.

Every function takes a view or table name (registered temp view, or a
catalog table) and returns a plain dictionary, so results are easy to log,
print, aggregate, or write out as JSON:

    {
        "Status": "Pass" or "Fail",
        "Description": "plain-English explanation of the result",
        ... a few extra keys depending on the check, e.g. sql, mismatch_count
    }

What's covered:

    source_target_count_check    - row counts match between source and target
    composite_key_check          - a set of columns forms a unique key
    duplicate_row_check          - no duplicate rows on chosen columns (also
                                    covers single-column uniqueness)
    mandatory_column_check       - required columns aren't null or blank
    comparison_check             - one or more columns match between source
                                    and target, row by row
    schema_validation            - table's columns and types match what's expected
    hardcode_column_check        - a column always holds one fixed value
    null_check                   - a column has no nulls
    accepted_values_check        - values fall within an allowed set
    range_check                  - numeric or date values fall within min/max
    regex_format_check           - values match a given pattern
    referential_integrity_check  - no orphaned foreign keys between two tables

The module doesn't depend on Databricks directly - it just uses whichever
SparkSession is already active, so the same code works in a notebook, a
scheduled job, or a plain spark-submit script.

Author: Alapan Barik
"""

import csv
import html
import json
import os
import re
from datetime import datetime

from pyspark.sql import SparkSession


# --------------------------------------------------------------------------- #
#  Internals
# --------------------------------------------------------------------------- #
def _spark():
    """Get the SparkSession that's already running.

    We fetch the active session instead of passing one into every function -
    keeps the check calls short, and matches how `spark` is already available
    as a global inside a Databricks notebook.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "No active SparkSession found. Start Spark before running validations."
        )
    return spark


def _as_list(columns):
    """Accept a comma-separated string or a list, return a clean list either way.

    Both styles show up in real pipelines, so this normalises once and we
    don't have to think about it again - mandatory_column_check("a,b,c") and
    mandatory_column_check(["a", "b"]) both work the same.
    """
    if isinstance(columns, str):
        return [c.strip() for c in columns.split(",") if c.strip()]
    return [str(c).strip() for c in columns]


def _count(sql):
    """Run a query and return how many rows it produced."""
    return _spark().sql(sql).count()


def _result(status, description, **extra):
    """Build the standard result dict every check returns."""
    out = {"Status": status, "Description": description}
    out.update(extra)
    return out


def _display_rows(df, label, limit=20):
    """Print up to `limit` offending rows to the screen and return them.
    """
    limited = df.limit(limit)
    print(f"\n--- {label} (showing up to {limit} row(s)) ---")
    try:
        display(limited)  # noqa: F821 - injected as a global by Databricks
    except NameError:
        limited.show(truncate=False)
    return [row.asDict() for row in limited.collect()]


# --------------------------------------------------------------------------- #
#  1. Reconciliation - Source vs Target row count
# --------------------------------------------------------------------------- #
def source_target_count_check(source_view, target_view, source_table_name=None, target_table_name=None):
    """Check that source and target have the same number of rows.

    If the ETL job isn't dropping or duplicating records, the two counts
    should match exactly. This is usually the first check to run - if the
    counts are off, there's no point comparing individual columns yet.
    Always prints a small on-screen table (one row per side, via a UNION ALL
    query) and an explicit "Mismatch count" line, in addition to returning
    both (`source_count`, `target_count`, `mismatch_count`) in the result
    dict. `source_view`/`target_view` are the registered temp view names used
    to run the query; pass `source_table_name`/`target_table_name` too if you
    want the on-screen table to show the real underlying table name (e.g.
    "raw_db.customer") instead of the view alias ("source"/"target").
    """
    source_count = _spark().table(source_view).count()
    target_count = _spark().table(target_view).count()
    mismatch_count = abs(source_count - target_count)

    if source_count == target_count:
        status = "Pass"
        verdict = "Source and Target count is matching"
    else:
        status = "Fail"
        verdict = "Source and Target count is NOT matching"

    source_label = source_table_name or source_view
    target_label = target_table_name or target_view
    count_sql = (
        f"SELECT '{source_label}' AS table_name, COUNT(*) AS row_count FROM {source_view} "
        f"UNION ALL "
        f"SELECT '{target_label}' AS table_name, COUNT(*) AS row_count FROM {target_view}"
    )
    _display_rows(_spark().sql(count_sql), "Source vs Target row count", limit=2)
    print(f"Mismatch count: {mismatch_count}")

    description = (
        f"Source Count:{source_count}\n"
        f" Target Count:{target_count} \n"
        f"{verdict}"
    )
    return _result(status, description,
                   source_count=source_count, target_count=target_count,
                   records_tested=source_count, mismatch_count=mismatch_count)


# --------------------------------------------------------------------------- #
#  2. Uniqueness - Composite / natural key
# --------------------------------------------------------------------------- #
def composite_key_check(view, key_columns, show_rows=True, row_limit=20):
    """Check that a set of columns uniquely identifies each row.

    Groups the table by the given key columns and flags any combination
    that shows up more than once. Also lists the columns that were left out
    of the key, so it's clear what the check did and didn't look at. When
    `show_rows` is True (the default) the offending key combinations are
    printed to the screen and returned under `duplicate_rows`, capped at
    `row_limit`.
    """
    key_columns = _as_list(key_columns)
    table = _spark().table(view)
    all_columns = table.columns
    non_key_columns = sorted(set(all_columns) - set(key_columns))
    records_tested = table.count()

    key_list = ",".join(key_columns)
    dup_sql = (
        f"SELECT {key_list}, COUNT(*) AS cnt "
        f"FROM {view} GROUP BY {key_list} HAVING COUNT(*) > 1"
    )
    duplicate_records = _count(dup_sql)
    duplicate_rows = []
    print(f"Mismatch count: {duplicate_records}")

    if duplicate_records == 0:
        status = "Pass"
        verdict = "Composite Key Check Passed."
    else:
        status = "Fail"
        verdict = "Composite Key Check Failed - duplicate keys found."
        if show_rows:
            duplicate_rows = _display_rows(
                _spark().sql(dup_sql),
                f"Composite Key Check - duplicate keys on {key_list}",
                row_limit,
            )

    description = (
        f"Given Colmns: {set(non_key_columns)} which is present in table is not "
        f"considered as part of composite check\n"
        f"Duplicate Record Count {duplicate_records}\n\n"
        f"{verdict}"
    )
    return _result(status, description,
                   duplicate_records=duplicate_records, sql=dup_sql,
                   duplicate_rows=duplicate_rows,
                   records_tested=records_tested, mismatch_count=duplicate_records)


# --------------------------------------------------------------------------- #
#  3. Uniqueness - Duplicate rows on chosen columns
# --------------------------------------------------------------------------- #
def duplicate_row_check(view, columns, show_rows=True, row_limit=20):
    """Check for duplicate rows across a set of columns.

    Similar to composite_key_check, but meant for one-off checks like "these
    columns together shouldn't repeat," rather than defining the table's
    actual grain. The SQL used is included in the result, so it can be
    re-run by hand if needed. When `show_rows` is True the duplicated
    combinations are printed to the screen and returned under
    `duplicate_rows`, capped at `row_limit`.
    """
    columns = _as_list(columns)
    col_list = ",".join(columns)
    records_tested = _spark().table(view).count()
    dup_sql = (
        f"SELECT {col_list},count(*) as cnt FROM {view} "
        f"group by {col_list} having count(*)>1"
    )
    duplicate_records = _count(dup_sql)
    duplicate_rows = []
    print(f"Mismatch count: {duplicate_records}")

    if duplicate_records == 0:
        status = "Pass"
        description = f"DuplicateRowCheck for {col_list} passed"
    else:
        status = "Fail"
        description = (
            f"DuplicateRowCheck for {col_list} failed. "
            f"Duplicate count: {duplicate_records}"
        )
        if show_rows:
            duplicate_rows = _display_rows(
                _spark().sql(dup_sql),
                f"Duplicate Row Check - duplicates on {col_list}",
                row_limit,
            )
    return _result(status, description, sql=dup_sql, duplicate_rows=duplicate_rows,
                   records_tested=records_tested, mismatch_count=duplicate_records)


# --------------------------------------------------------------------------- #
#  4. Completeness - Mandatory (not null / not blank) columns
# --------------------------------------------------------------------------- #
def mandatory_column_check(view, columns, show_rows=True, row_limit=20):
    """Check that required columns are never null or blank.

    For fields that must always be filled in (name, date of birth, country,
    and so on), this counts rows where the value is NULL or, after trimming
    whitespace, an empty string. Any column with a non-zero count fails, and
    the per-column counts are returned so it's easy to see which fields are
    the problem. When `show_rows` is True the offending rows for each
    failing column are printed to the screen and returned under
    `mismatched_rows` (keyed by column name), capped at `row_limit` each.
    """
    columns = _as_list(columns)
    records_tested = _spark().table(view).count()
    overall = "Pass"
    lines = []
    mismatches = {}
    mismatched_rows = {}

    for col in columns:
        where = f"{col} IS NULL OR TRIM(CAST({col} AS STRING)) = ''"
        mismatch = _spark().sql(
            f"SELECT COUNT(*) AS cnt FROM {view} WHERE {where}"
        ).collect()[0]["cnt"]
        mismatches[col] = mismatch

        if mismatch > 0:
            overall = "Fail"
            lines.append(
                f"Target Column:{col} Mandatory validation failed. "
                f"Mismatch count: {mismatch}"
            )
            if show_rows:
                mismatched_rows[col] = _display_rows(
                    _spark().sql(f"SELECT * FROM {view} WHERE {where}"),
                    f"Mandatory Column Check - {col} is null/blank",
                    row_limit,
                )
        else:
            lines.append(f"Target Column:{col} Mandatory validation passed.")

    total_mismatch = sum(mismatches.values())
    print(f"Mismatch count: {total_mismatch}")

    return _result(overall, "\n".join(lines), mismatch_counts=mismatches,
                   mismatched_rows=mismatched_rows,
                   records_tested=records_tested, mismatch_count=total_mismatch)


# --------------------------------------------------------------------------- #
#  5. Accuracy - Column-by-column source vs target comparison
# --------------------------------------------------------------------------- #


def comparison_check(source_view, target_view, source_col, target_col, key_column,
                      show_rows=True, row_limit=20):
    """Compare one or more columns between source and target, joined on a key.

    A matching row count doesn't prove the values themselves came through
    correctly. This joins source to target on the key column and counts rows
    where any of the given columns differ. It uses Spark's null-safe equality
    (`<=>`), so (NULL, NULL) counts as a match and (NULL, 'x') counts as a
    mismatch - which is what you want when comparing real ETL output.

    "Records to test" is the count of non-null values for the column(s)
    being compared, in the target - not the target's total row count, since
    a column that's legitimately NULL for a lot of rows shouldn't inflate
    that number.

    `source_col` / `target_col` each take a single column name, a
    comma-separated string, or a list - so the same function covers a
    one-column spot check and a full multi-column row comparison. Pass them
    in the same order on both sides; a row fails if any one of the paired
    columns doesn't match. When `show_rows` is True the actual mismatching
    rows (key + each source/target value side by side) are printed to the
    screen and returned under `mismatched_rows`, capped at `row_limit`.
    """
    source_cols = _as_list(source_col)
    target_cols = _as_list(target_col)
    if len(source_cols) != len(target_cols):
        raise ValueError("source_col and target_col must list the same number of columns")

    non_null_counts = [
        _spark().sql(f"SELECT COUNT({tc}) AS cnt FROM {target_view}").collect()[0]["cnt"]
        for tc in target_cols
    ]
    records_to_test = non_null_counts[0] if len(non_null_counts) == 1 else sum(non_null_counts)

    mismatch_conditions = " OR ".join(
        f"NOT (s.{sc} <=> t.{tc})" for sc, tc in zip(source_cols, target_cols)
    )
    mismatch_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {source_view} s
        JOIN {target_view} t
          ON s.{key_column} = t.{key_column}
        WHERE {mismatch_conditions}
    """
    mismatch_count = _spark().sql(mismatch_sql).collect()[0]["cnt"]

    col_list = ", ".join(
        sc if sc == tc else f"{sc}->{tc}" for sc, tc in zip(source_cols, target_cols)
    )
    mismatched_rows = []

    if len(source_cols) == 1:
        sc, tc = source_cols[0], target_cols[0]
        if mismatch_count == 0:
            status = "Pass"
            message = (
                f"There are {records_to_test} number of records to test for '{sc}'\n"
                f"Comparison Check for source column: {sc} and Target column {tc} PASSED"
            )
        else:
            status = "Fail"
            message = (
                f"There are {records_to_test} number of records to test for '{sc}'\n"
                f"Comparison Check for source column: {sc} and Target column {tc} FAILED\n"
                f"Mismatch count : {mismatch_count}"
            )
    else:
        if mismatch_count == 0:
            status = "Pass"
            message = (
                f"There are {records_to_test} number of records to test for {col_list}\n"
                f"Comparison Check for columns {col_list} PASSED"
            )
        else:
            status = "Fail"
            message = (
                f"There are {records_to_test} number of records to test for {col_list}\n"
                f"Comparison Check for columns {col_list} FAILED\n"
                f"Mismatch count : {mismatch_count}"
            )

    print(message)

    if status == "Fail" and show_rows:
        select_cols = ", ".join(
            [f"s.{key_column} AS {key_column}"]
            + [f"s.{sc} AS source_{sc}" for sc in source_cols]
            + [f"t.{tc} AS target_{tc}" for tc in target_cols]
        )
        rows_sql = f"""
            SELECT {select_cols}
            FROM {source_view} s
            JOIN {target_view} t
              ON s.{key_column} = t.{key_column}
            WHERE {mismatch_conditions}
        """
        mismatched_rows = _display_rows(
            _spark().sql(rows_sql),
            f"Comparison Check - mismatches on {col_list}",
            row_limit,
        )

    return _result(status, message,
                   records_tested=records_to_test, mismatch_count=mismatch_count,
                   mismatched_rows=mismatched_rows)


# --------------------------------------------------------------------------- #
#  6. Metadata - Schema / data type contract
# --------------------------------------------------------------------------- #
def schema_validation(view, expected_schema):
    """Check a table's columns and data types against what's expected.

    `expected_schema` is a dict like {"customer_id": "string", "date_of_birth":
    "date"}. If everything matches exactly, it says so; otherwise it falls
    back to checking just the columns you passed in, one by one, and reports
    which ones are missing or the wrong type. Column names and type strings
    are compared case-insensitively.
    """
    actual = {
        f.name.lower(): f.dataType.simpleString()
        for f in _spark().table(view).schema.fields
    }
    expected = {k.lower(): v.lower() for k, v in expected_schema.items()}

    schemas_identical = (
        set(actual.keys()) == set(expected.keys())
        and all(actual[c] == expected[c] for c in expected)
    )

    lines = []
    if schemas_identical:
        lines.append("Both schema are same.")
    else:
        lines.append("Both schema are not same.")
        lines.append(
            "Proceeding with partial Validation i.e. Validating data type of the "
            "column being passed."
        )

    status = "Pass"
    mismatch_count = 0
    for col, exp_type in expected.items():
        if col not in actual:
            status = "Fail"
            mismatch_count += 1
            lines.append(f"Given column {col} is not present in table.")
        elif actual[col] == exp_type:
            lines.append(f"Given column {col} datatype {exp_type} matched.")
        else:
            status = "Fail"
            mismatch_count += 1
            lines.append(
                f"Given column {col} datatype mismatch. "
                f"Expected {exp_type}, found {actual[col]}."
            )

    print(f"Mismatch count: {mismatch_count}")
    return _result(status, "\n".join(lines),
                   records_tested=len(expected), mismatch_count=mismatch_count)


# --------------------------------------------------------------------------- #
#  7. Validity - Hard-coded / constant column value
# --------------------------------------------------------------------------- #
def hardcode_column_check(view, column, expected_value):
    """Check that a column only ever holds one fixed value.

    Useful for columns that should be constant for a given run - a source
    system tag, a load flag, a region code, that kind of thing. Any row
    where the value doesn't match `expected_value`, or is NULL, fails.
    """
    records_tested = _spark().table(view).count()
    mismatch = _spark().sql(
        f"SELECT COUNT(*) AS cnt FROM {view} "
        f"WHERE {column} IS NULL OR CAST({column} AS STRING) <> '{expected_value}'"
    ).collect()[0]["cnt"]
    print(f"Mismatch count: {mismatch}")

    if mismatch == 0:
        status = "Pass"
        description = (
            f"HardcodeColumnCheck for {column} passed. "
            f"All rows equal '{expected_value}'."
        )
    else:
        status = "Fail"
        description = (
            f"HardcodeColumnCheck for {column} failed. "
            f"{mismatch} row(s) not equal to '{expected_value}'."
        )
    return _result(status, description, mismatch_count=mismatch, records_tested=records_tested)


# --------------------------------------------------------------------------- #
#  A few more checks I use often enough to keep handy
# --------------------------------------------------------------------------- #
def null_check(view, columns):
    """Check that the given columns contain no NULLs.

    Unlike mandatory_column_check, this only cares about NULL, not empty
    strings - useful for numeric or date columns where an empty string
    wouldn't be a valid value anyway.
    """
    columns = _as_list(columns)
    overall = "Pass"
    lines = []
    for col in columns:
        nulls = _count(f"SELECT 1 FROM {view} WHERE {col} IS NULL")
        if nulls > 0:
            overall = "Fail"
            lines.append(f"Column {col} has {nulls} NULL value(s).")
        else:
            lines.append(f"Column {col} has no NULL values.")
    return _result(overall, "\n".join(lines))


def accepted_values_check(view, column, accepted_values):
    """Check that a column only contains values from an allowed set.

    Good fit for status codes, flags, or category columns. Anything outside
    `accepted_values` counts as a violation.
    """
    quoted = ",".join(f"'{v}'" for v in accepted_values)
    bad_sql = (
        f"SELECT COUNT(*) AS cnt FROM {view} "
        f"WHERE {column} IS NOT NULL AND CAST({column} AS STRING) NOT IN ({quoted})"
    )
    violations = _spark().sql(bad_sql).collect()[0]["cnt"]
    if violations == 0:
        return _result("Pass",
                       f"All values of {column} are within {accepted_values}.")
    return _result(
        "Fail",
        f"{violations} row(s) of {column} fall outside {accepted_values}.",
        mismatch_count=violations,
    )


def range_check(view, column, min_value=None, max_value=None):
    """Check that numeric or date values fall inside a min/max range.

    Either bound can be left out for an open-ended range. Rows below the
    minimum or above the maximum count as violations.
    """
    conditions = []
    if min_value is not None:
        conditions.append(f"{column} < {min_value}")
    if max_value is not None:
        conditions.append(f"{column} > {max_value}")
    if not conditions:
        raise ValueError("range_check needs at least one of min_value / max_value")

    where = " OR ".join(conditions)
    violations = _count(f"SELECT 1 FROM {view} WHERE {column} IS NOT NULL AND ({where})")
    bounds = f"[{min_value}, {max_value}]"
    if violations == 0:
        return _result("Pass", f"All values of {column} are within {bounds}.")
    return _result(
        "Fail",
        f"{violations} value(s) of {column} fall outside {bounds}.",
        mismatch_count=violations,
    )


def regex_format_check(view, column, pattern):
    """Check that every non-null value in a column matches a regex pattern.

    Good for emails, phone numbers, postal codes, or any field with a fixed
    format. Runs on Spark's RLIKE under the hood.
    """
    safe_pattern = pattern.replace("'", "\\'")
    violations = _count(
        f"SELECT 1 FROM {view} "
        f"WHERE {column} IS NOT NULL AND {column} NOT RLIKE '{safe_pattern}'"
    )
    if violations == 0:
        return _result("Pass", f"All values of {column} match /{pattern}/.")
    return _result(
        "Fail",
        f"{violations} value(s) of {column} do not match /{pattern}/.",
        mismatch_count=violations,
    )


def referential_integrity_check(child_view, child_col, parent_view, parent_col):
    """Check that every child key exists in the parent table (no orphans).

    Standard foreign-key check: values in `child_view.child_col` must show
    up in `parent_view.parent_col`. NULL child keys are skipped, same as
    normal FK behaviour.
    """
    orphan_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {child_view} c
        LEFT JOIN {parent_view} p
          ON c.{child_col} = p.{parent_col}
        WHERE c.{child_col} IS NOT NULL AND p.{parent_col} IS NULL
    """
    orphans = _spark().sql(orphan_sql).collect()[0]["cnt"]
    if orphans == 0:
        return _result(
            "Pass",
            f"All {child_view}.{child_col} values exist in "
            f"{parent_view}.{parent_col}.",
        )
    return _result(
        "Fail",
        f"{orphans} orphan value(s) in {child_view}.{child_col} not found in "
        f"{parent_view}.{parent_col}.",
        mismatch_count=orphans,
    )


# --------------------------------------------------------------------------- #
#  Reporting - export the collected results to any file format
# --------------------------------------------------------------------------- #
def _report_rows(data):
    """Flatten the results dict into a list of {Check, Status, Description} rows."""
    rows = []
    for check, result in data.items():
        if isinstance(result, dict):
            rows.append({
                "Check": check,
                "Status": result.get("Status", ""),
                "Description": result.get("Description", ""),
            })
        else:
            rows.append({"Check": check, "Status": "", "Description": str(result)})
    return rows


def print_summary(data):
    """Print the Pass/Fail summary block that closes out a validation run.

    Loops over the same results dict the executor builds up (`data`) and
    prints one line per check plus an X/N tally, so the last thing a run
    prints to the screen is a quick glance at what passed and what didn't -
    handy on top of the full per-check output each check already prints.
    """
    rows = _report_rows(data)
    passed = sum(1 for r in rows if r["Status"] == "Pass")
    failed = [r["Check"] for r in rows if r["Status"] != "Pass"]

    banner = " VALIDATION SUMMARY "
    width = 60
    print(banner.center(width, "="))
    for r in rows:
        print(f"  {r['Status']:<6} {r['Check']}")
    print(f"  {passed}/{len(rows)} checks passed")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    print("=" * width)
    return {"passed": passed, "total": len(rows), "failed": failed}


_JOB_PREFIX_RE = re.compile(r"^\[(?P<job>[^\]]+)\]\s*(?P<name>.+)$")


def _check_metrics(result):
    """Pull (tested, mismatch) counts out of a check's result dict.

    Every check in this module now returns `records_tested` /
    `mismatch_count`; this just defaults to 0/0 for anything that doesn't
    (e.g. a purely descriptive "ADF Trigger Check" record), so the HTML
    dashboard can render a bar for it without special-casing every caller.
    """
    tested = result.get("records_tested")
    if tested is None:
        tested = result.get("target_count", 0)
    mismatch = result.get("mismatch_count")
    if mismatch is None:
        mismatch = result.get("duplicate_records", 0)
    return int(tested or 0), int(mismatch or 0)


def _sample_tables(result):
    """Pull whatever sample mismatching/duplicate rows a check recorded, as a
    list of (subtitle, rows) pairs ready to render as HTML tables.
    `mandatory_column_check` keys `mismatched_rows` by column, so each column
    becomes its own labelled table; everything else is a flat row list.
    """
    tables = []
    dup_rows = result.get("duplicate_rows")
    if dup_rows:
        tables.append((None, dup_rows))
    mismatched = result.get("mismatched_rows")
    if isinstance(mismatched, dict):
        for col, rows in mismatched.items():
            if rows:
                tables.append((col, rows))
    elif mismatched:
        tables.append((None, mismatched))
    return tables


def _rows_to_html_table(rows):
    """Render a list of row-dicts (as returned by `_display_rows`) as an HTML
    table. Takes the union of keys across all rows (in first-seen order),
    not just the first row's - a combined "Comparison Check" sample can mix
    rows from different compared columns (e.g. one row's mismatch is on
    `last_name`, another's is on `email`), each with a different key set.
    """
    if not rows:
        return ""
    cols = []
    for r in rows:
        for k in r.keys():
            if k not in cols:
                cols.append(k)
    thead = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(r.get(c, '')))}</td>" for c in cols) + "</tr>"
        for r in rows
    )
    return f"<table class='sample-table'><thead><tr>{thead}</tr></thead><tbody>{body}</tbody></table>"


_DASHBOARD_CSS = """
* { box-sizing: border-box; }
body { font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
       background:#f4f5f8; margin:0; padding:24px; color:#1a1a1a; }
.header h1 { margin:0 0 4px 0; font-size:20px; }
.header .run-at { color:#3b6fd1; font-size:12px; margin-bottom:6px; }
.header .jobs-line { font-size:12px; margin-bottom:16px; }
.summary-cards { display:flex; gap:16px; margin-bottom:16px; flex-wrap:wrap; }
.card { background:#fff; border:1px solid #e3e5eb; border-radius:8px; flex:1; min-width:140px;
        padding:20px; text-align:center; box-shadow:0 1px 2px rgba(0,0,0,.04); }
.metric { font-size:28px; font-weight:700; }
.metric.pass { color:#2e7d32; }
.metric.fail { color:#c62828; }
.metric-label { font-size:11px; letter-spacing:.05em; color:#8a8f98; margin-top:4px; }
.donut-card { display:flex; align-items:center; justify-content:center; }
.donut { width:88px; height:88px; border-radius:50%; display:flex; align-items:center; justify-content:center; }
.donut-hole { width:60px; height:60px; border-radius:50%; background:#fff; display:flex;
              flex-direction:column; align-items:center; justify-content:center; }
.donut-pct { font-size:15px; font-weight:700; }
.donut-label { font-size:8px; color:#8a8f98; letter-spacing:.04em; }
.section { background:#fff; border:1px solid #e3e5eb; border-radius:8px; padding:20px; margin-bottom:16px; }
.section-title { font-weight:700; margin-bottom:12px; }
.check-row { display:flex; align-items:center; gap:12px; padding:4px 0; font-size:12px; }
.check-name { flex:0 0 260px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#2f5fc4; }
.check-bar { flex:1; height:10px; border-radius:5px; overflow:hidden; display:flex; background:#eee; }
.bar-pass { background:#2e7d32; height:100%; }
.bar-fail { background:#c62828; height:100%; }
.check-count { flex:0 0 210px; text-align:right; }
.check-count.pass { color:#2e7d32; }
.check-count.fail { color:#c62828; }
.detail-card { border-left:4px solid #c62828; background:#fafafa; border-radius:6px;
               padding:14px 16px; margin-bottom:12px; }
.detail-card.pass { border-left-color:#2e7d32; }
.detail-top { display:flex; align-items:center; gap:10px; margin-bottom:6px; flex-wrap:wrap; }
.badge { font-size:11px; font-weight:700; padding:2px 8px; border-radius:4px; color:#fff; }
.badge.pass { background:#2e7d32; }
.badge.fail { background:#c62828; }
.detail-title { font-weight:600; }
.detail-meta { margin-left:auto; font-size:11px; color:#8a8f98; }
.detail-desc { font-size:12.5px; color:#333; margin:6px 0; }
.count-table, .sample-table { border-collapse:collapse; margin:8px 0; font-size:12px; }
.count-table th, .count-table td, .sample-table th, .sample-table td {
  border:1px solid #e3e5eb; padding:5px 9px; text-align:left; }
.sample-title { font-size:11px; color:#8a8f98; margin-top:8px; }
"""


def _render_html_dashboard(data, title="Validation Report"):
    """Build the dashboard-style HTML report: summary cards, one proportional
    Pass/Fail bar per check under "Checks overview" (bar width is driven by
    `mismatch / tested`, not just a solid colour by Status - a check that
    fails on 6 of 1080 rows gets a mostly-green bar with a thin red sliver,
    not a solid red bar), and per-check detail cards with description +
    sample mismatching/duplicate rows. Self-contained: inline CSS, no
    external assets, so the single .html file is the whole report.
    """
    entries = []
    for check_name, result in data.items():
        if not isinstance(result, dict):
            result = {"Status": "", "Description": str(result)}
        m = _JOB_PREFIX_RE.match(check_name)
        job = m.group("job") if m else None
        tested, mismatch = _check_metrics(result)
        entries.append({
            "check_name": check_name, "job": job,
            "status": result.get("Status", ""), "description": result.get("Description", ""),
            "tested": tested, "mismatch": mismatch, "result": result,
        })

    total_checks = len(entries)
    passed = sum(1 for e in entries if e["status"] == "Pass")
    failed = total_checks - passed
    pass_rate = (passed / total_checks * 100) if total_checks else 0.0
    records_tested_total = sum(e["tested"] for e in entries)
    mismatches_total = sum(e["mismatch"] for e in entries)
    jobs = list(dict.fromkeys(e["job"] for e in entries if e["job"]))
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    overview_rows = []
    for e in entries:
        tested, mismatch = e["tested"], e["mismatch"]
        if tested > 0:
            fail_pct = min(100.0, mismatch / tested * 100)
        else:
            fail_pct = 100.0 if e["status"] == "Fail" else 0.0
        pass_pct = 100.0 - fail_pct
        status_class = "pass" if e["status"] == "Pass" else "fail"
        overview_rows.append(
            "<div class='check-row'>"
            f"<div class='check-name'>{html.escape(e['check_name'])}</div>"
            "<div class='check-bar'>"
            f"<div class='bar-pass' style='width:{pass_pct:.2f}%'></div>"
            f"<div class='bar-fail' style='width:{fail_pct:.2f}%'></div>"
            "</div>"
            f"<div class='check-count {status_class}'>{html.escape(e['status'])} {mismatch} mismatch / {tested} tested</div>"
            "</div>"
        )

    detail_cards = []
    for e in entries:
        result = e["result"]
        status_class = "pass" if e["status"] == "Pass" else "fail"
        tested, mismatch = e["tested"], e["mismatch"]
        check_pass_rate = ((tested - mismatch) / tested * 100) if tested else (100.0 if e["status"] == "Pass" else 0.0)

        count_table_html = ""
        if "source_count" in result and "target_count" in result:
            count_table_html = (
                "<table class='count-table'><thead><tr><th>Source Count</th><th>Target Count</th></tr></thead>"
                f"<tbody><tr><td>{result['source_count']}</td><td>{result['target_count']}</td></tr></tbody></table>"
            )

        sample_html = ""
        for subtitle, rows in _sample_tables(result):
            label = f"{html.escape(str(subtitle))} - " if subtitle else ""
            sample_html += (
                f"<div class='sample-title'>{label}mismatching rows shown ({len(rows)} row(s))</div>"
                + _rows_to_html_table(rows)
            )

        desc_html = html.escape(e["description"]).replace("\n", "<br>")
        detail_cards.append(
            f"<div class='detail-card {status_class}'>"
            "<div class='detail-top'>"
            f"<span class='badge {status_class}'>{html.escape(e['status'])}</span>"
            f"<span class='detail-title'>{html.escape(e['check_name'])}</span>"
            f"<span class='detail-meta'>Tested: {tested} | Mismatches: {mismatch} | Pass rate: {check_pass_rate:.2f}%</span>"
            "</div>"
            f"{count_table_html}"
            f"<div class='detail-desc'>{desc_html}</div>"
            f"{sample_html}"
            "</div>"
        )

    jobs_line = f"<div class='jobs-line'><b>Jobs:</b> {html.escape(', '.join(jobs))}</div>" if jobs else ""

    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{html.escape(title)}</title><style>{_DASHBOARD_CSS}</style></head><body>"
        "<div class='header'>"
        f"<h1>{html.escape(title)}</h1>"
        f"<div class='run-at'>Run at {run_at}</div>"
        f"{jobs_line}"
        "</div>"
        "<div class='summary-cards'>"
        "<div class='card donut-card'>"
        f"<div class='donut' style='background: conic-gradient(#2e7d32 0% {pass_rate:.2f}%, #c62828 {pass_rate:.2f}% 100%);'>"
        f"<div class='donut-hole'><div class='donut-pct'>{pass_rate:.0f}%</div><div class='donut-label'>PASS RATE</div></div>"
        "</div></div>"
        f"<div class='card'><div class='metric'>{total_checks}</div><div class='metric-label'>TOTAL CHECKS</div></div>"
        f"<div class='card'><div class='metric pass'>{passed}</div><div class='metric-label'>PASSED</div></div>"
        f"<div class='card'><div class='metric fail'>{failed}</div><div class='metric-label'>FAILED</div></div>"
        f"<div class='card'><div class='metric {'pass' if pass_rate == 100 else 'fail'}'>{pass_rate:.1f}%</div><div class='metric-label'>PASS RATE</div></div>"
        f"<div class='card'><div class='metric'>{records_tested_total:,}</div><div class='metric-label'>RECORDS TESTED</div></div>"
        "</div>"
        "<div class='summary-cards'>"
        f"<div class='card'><div class='metric'>{mismatches_total:,}</div><div class='metric-label'>TOTAL MISMATCHES</div></div>"
        "</div>"
        "<div class='section'>"
        "<div class='section-title'>Checks overview</div>"
        f"{''.join(overview_rows)}"
        "</div>"
        "<div class='section'>"
        "<div class='section-title'>Check details &amp; mismatch samples</div>"
        f"{''.join(detail_cards)}"
        "</div>"
        "</body></html>"
    )


def export_report(data, path, fmt=None, title="Validation Report"):
    """Write the results dict to a file, in whichever format you need.

    The format comes from `fmt` if you pass it, otherwise it's guessed from
    the file extension. Supports json, csv, txt, md, html, xlsx (xlsx needs
    `pandas` + `openpyxl` installed). `title` is only used for the html
    dashboard report. Returns the path it wrote to.

    `data` is the same dict the executor builds up -
    {check_name: {"Status": ..., "Description": ...}}.
    """
    fmt = (fmt or os.path.splitext(path)[1].lstrip(".")).lower()
    rows = _report_rows(data)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if fmt == "json":
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    elif fmt == "csv":
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Check", "Status", "Description"])
            writer.writeheader()
            writer.writerows(rows)

    elif fmt in ("txt", "text"):
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(f"[{r['Status']}] {r['Check']}\n{r['Description']}\n\n")

    elif fmt in ("md", "markdown"):
        with open(path, "w", encoding="utf-8") as f:
            f.write("| Check | Status | Description |\n|---|---|---|\n")
            for r in rows:
                desc = r["Description"].replace("\n", "<br>").replace("|", "\\|")
                f.write(f"| {r['Check']} | {r['Status']} | {desc} |\n")

    elif fmt in ("html", "htm"):
        with open(path, "w", encoding="utf-8") as f:
            f.write(_render_html_dashboard(data, title=title))

    elif fmt in ("xlsx", "xls"):
        try:
            import pandas as pd
            pd.DataFrame(rows).to_excel(path, index=False)
        except ImportError as exc:
            raise RuntimeError("xlsx export needs pandas + openpyxl installed.") from exc

    else:
        raise ValueError(
            f"Unsupported report format '{fmt}'. "
            "Use one of: json, csv, txt, md, html, xlsx."
        )

    return path


def report_to_spark_dataframe(spark, data, **extra_columns):
    """Turn the results dict into a Spark DataFrame, for writing to any sink.

    Any keyword arguments (e.g. snap_date=..., run_id=...) get added as
    constant columns, so the report carries its own context. Write it out
    however fits the pipeline:

        df = report_to_spark_dataframe(spark, data, snap_date=snap_date)
        df.write.format("delta").mode("overwrite").saveAsTable("...")
        df.write.mode("overwrite").parquet("/mnt/reports/...")
        df.write.option("header", True).csv("/mnt/reports/...")
    """
    rows = _report_rows(data)
    for r in rows:
        r.update({k: str(val) for k, val in extra_columns.items()})
    return spark.createDataFrame(rows)


# --------------------------------------------------------------------------- #
#  Backwards-compatible names
#  executor.py (the original AMLQU/C360 notebook) calls these PascalCase names
#  via `%run ./validator` and must keep working unmodified, so `DuplicateColumnCheck`
#  stays here even though the function underneath is `duplicate_row_check`.
#  `DuplicateRowCheck` is the same alias under its accurate name - prefer it
#  (or the snake_case `duplicate_row_check`) in new code, e.g. executor-generic.py.
# --------------------------------------------------------------------------- #
SourceCountCheck = source_target_count_check
DuplicateColumnCheck = duplicate_row_check
DuplicateRowCheck = duplicate_row_check
MandatoryColumnCheck = mandatory_column_check
HardcodeColumnCheck = hardcode_column_check
table_schema_validation = schema_validation


def ComparisonCheck(source_view, target_view, source_col, target_col, key_column,
                     show_rows=True, row_limit=20):
    """String-returning shim over comparison_check().

    The original notebook does `data_check += ComparisonCheck(...)` and then
    checks `if "failed" in data_check` (lowercase). comparison_check()'s own
    Description now reads "... PASSED"/"... FAILED" (uppercase, a newer,
    more explicit format), which wouldn't match that substring check, so
    this shim builds its own legacy-worded string from the Status instead of
    returning comparison_check()'s Description directly - keeps executor.py
    working unmodified. Use comparison_check() directly in new code if you
    want the full result dict (mismatch_count, mismatched_rows, ...).
    """
    result = comparison_check(
        source_view, target_view, source_col, target_col, key_column,
        show_rows=show_rows, row_limit=row_limit,
    )
    col_list = ", ".join(_as_list(source_col))
    if result["Status"] == "Pass":
        return f"ComparisonCheck for columns {col_list} passed \n"
    return (
        f"ComparisonCheck for columns {col_list} failed. \n"
        f"Mismatch count: {result['mismatch_count']}\n"
    )


# --------------------------------------------------------------------------- #
#  Convenience - which names are exported
# --------------------------------------------------------------------------- #
__all__ = [
    # snake_case API
    "source_target_count_check",
    "composite_key_check",
    "duplicate_row_check",
    "mandatory_column_check",
    "comparison_check",
    "schema_validation",
    "hardcode_column_check",
    "null_check",
    "accepted_values_check",
    "range_check",
    "regex_format_check",
    "referential_integrity_check",
    # reporting
    "print_summary",
    "export_report",
    "report_to_spark_dataframe",
    # backwards-compatible PascalCase names used by the executor notebook
    "SourceCountCheck",
    "DuplicateColumnCheck",
    "DuplicateRowCheck",
    "MandatoryColumnCheck",
    "HardcodeColumnCheck",
    "ComparisonCheck",
    "table_schema_validation",
]