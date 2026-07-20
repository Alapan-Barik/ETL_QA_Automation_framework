# Databricks notebook source
"""
File_Reader.py
===============

Reusable, configurable file parser for File-to-Table / Table-to-File
validation. Turns a CSV, TXT, JSON, pipe/tab-delimited, or fixed-width file
into a clean Spark DataFrame with the same shape a database table would
have - so `Data_Validator.py`'s checks (which only ever look at a registered
temp view / table name) can run against it completely unmodified.

    FileReader.read_file(config) -> pyspark.sql.DataFrame

`config` is a plain dict - same style as `EXPECTED_SCHEMA` / the other
config blocks in `Executor.py`. Every key is optional except `file_path`:

    {
        "file_path":     "/path/to/file.csv",   # required
        "label":         "source",              # only used in log lines, default "file"
        "file_type":     "csv",                 # csv | txt | json | pipe | tab | fixed_width
                                                 # auto-detected from the extension if omitted
        "delimiter":     ",",                   # csv/txt field separator
        "encoding":      "utf-8",
        "quote_char":    '"',
        "escape_char":   "\\",
        "header":        True,                  # False when the file has no header row
        "skip_rows":     0,                     # metadata/banner lines above the real data
        "footer_rows":   0,                     # trailer/summary lines below the real data
        "comment_prefix": None,                 # e.g. "#" - lines starting with it are dropped
        "column_names":  None,                  # explicit column list (required when header=False
                                                  #   for an irregular layout, or to rename _c0.._cN)
        "column_mapping": None,                 # {"raw_name": "clean_name"}
        "dtype_mapping":  None,                 # {"column": "int"/"double"/"string"/"date"/...}
        "null_values":    None,                 # ["NA", "NULL", "--"] -> treated as real nulls
        "trim_spaces":    True,                 # strip whitespace on every string column
        "colspecs":       None,                 # fixed_width only: [("name", start, length), ...]
        "json_lines":     True,                 # JSON Lines (one object per line) - the default
        "multiline_json": False,                # a single JSON array/object spanning many lines
        "sheet_name":     None,                 # reserved for future Excel support
    }

Every parser (csv / txt / json / fixed-width) funnels through the same
"standardisation" pass at the end - column renaming, duplicate-column
handling, null normalisation, trimming, and dtype casting - so the caller
never needs to know which parser actually produced the DataFrame.

Author: Alapan Barik
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# --------------------------------------------------------------------------- #
#  Internals
# --------------------------------------------------------------------------- #
def _spark():
    """Get the SparkSession that's already running (same helper contract as
    Data_Validator.py's `_spark()` - no session is created here)."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "No active SparkSession found. Start Spark before reading a file."
        )
    return spark


_DEFAULTS = {
    "label": "file",
    "file_type": None,
    "delimiter": ",",
    "encoding": "utf-8",
    "quote_char": '"',
    "escape_char": "\\",
    "header": True,
    "skip_rows": 0,
    "footer_rows": 0,
    "comment_prefix": None,
    "column_names": None,
    "column_mapping": None,
    "dtype_mapping": None,
    "null_values": None,
    "trim_spaces": True,
    "colspecs": None,
    "json_lines": True,
    "multiline_json": False,
    "sheet_name": None,
}

_TYPE_ALIASES = {
    "pipe": ("csv", "|"),
    "tab": ("csv", "\t"),
}


def _resolve_config(config):
    """Merge the caller's config over the defaults and fill in file_type /
    delimiter from the path extension or a shorthand alias ("pipe", "tab")."""
    if "file_path" not in config or not config["file_path"]:
        raise ValueError("File_Reader config is missing required key 'file_path'.")

    cfg = dict(_DEFAULTS)
    cfg.update(config)

    file_type = (cfg["file_type"] or os.path.splitext(cfg["file_path"])[1].lstrip(".")).lower()
    if file_type in _TYPE_ALIASES:
        file_type, alias_delimiter = _TYPE_ALIASES[file_type]
        if "delimiter" not in config:
            cfg["delimiter"] = alias_delimiter
    if file_type in ("txt",):
        file_type = "csv"  # a delimited TXT feed is parsed exactly like CSV
    cfg["file_type"] = file_type
    return cfg


def _check_file_exists(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File_Reader could not find file: {path}")
    if os.path.isfile(path) and os.path.getsize(path) == 0:
        raise RuntimeError(f"File_Reader: '{path}' is empty (0 bytes).")


def _raw_lines(path, encoding, skip_rows, footer_rows, comment_prefix):
    """Read a text file into an ordered list of lines with `skip_rows`
    leading lines, `footer_rows` trailing lines, and any comment-prefixed
    lines removed - the shared building block behind CSV/TXT and JSON-Lines
    parsing, so metadata banners and trailer/summary rows never reach the
    real parser.
    """
    lines_df = _spark().read.option("encoding", encoding).text(path)
    indexed = lines_df.rdd.zipWithIndex().toDF(["row", "idx"])
    total = indexed.count()

    kept = indexed.filter(
        (F.col("idx") >= skip_rows) & (F.col("idx") < total - footer_rows)
    ).orderBy("idx")

    lines = [r["row"]["value"] for r in kept.collect()]
    if comment_prefix:
        lines = [ln for ln in lines if not ln.lstrip().startswith(comment_prefix)]
    return lines


# --------------------------------------------------------------------------- #
#  Format-specific parsers
# --------------------------------------------------------------------------- #
def _read_csv(cfg):
    """CSV / pipe / tab / delimited-TXT parser.

    `skip_rows`, `footer_rows` and `comment_prefix` are applied on the raw
    text first (Spark's CSV reader has no native support for arbitrary
    leading/trailing junk rows), then the cleaned lines are handed to
    Spark's own CSV parser so delimiter/quote/escape/header handling stays
    exactly as robust as `spark.read.csv()` already is.
    """
    lines = _raw_lines(
        cfg["file_path"], cfg["encoding"], cfg["skip_rows"], cfg["footer_rows"], cfg["comment_prefix"]
    )
    if not lines:
        raise RuntimeError(f"File_Reader: no data rows left in '{cfg['file_path']}' after skip/footer/comment filtering.")

    reader = (
        _spark().read
        .option("header", str(bool(cfg["header"])).lower())
        .option("delimiter", cfg["delimiter"])
        .option("quote", cfg["quote_char"])
        .option("escape", cfg["escape_char"])
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
    )
    if cfg["null_values"]:
        reader = reader.option("nullValue", cfg["null_values"][0])

    try:
        df = reader.csv(_spark().sparkContext.parallelize(lines))
    except Exception as exc:
        raise RuntimeError(f"File_Reader: failed parsing CSV/TXT file '{cfg['file_path']}': {exc}") from exc
    return df


def _read_json(cfg):
    """JSON / JSON-Lines parser.

    `json_lines=True` (the default) treats the file as one JSON object per
    line, so `skip_rows`/`footer_rows`/`comment_prefix` can be applied the
    same way as CSV. `multiline_json=True` reads a single JSON array/object
    spanning the whole file via Spark's `multiline` option - skip/footer
    trimming is skipped in that mode since slicing raw lines out of a single
    JSON document would corrupt it.
    """
    if cfg["multiline_json"]:
        if cfg["skip_rows"] or cfg["footer_rows"]:
            print("Applying parser... (skip_rows/footer_rows are ignored for multiline_json)")
        df = _spark().read.option("multiLine", "true").option("encoding", cfg["encoding"]).json(cfg["file_path"])
    else:
        lines = _raw_lines(
            cfg["file_path"], cfg["encoding"], cfg["skip_rows"], cfg["footer_rows"], cfg["comment_prefix"]
        )
        if not lines:
            raise RuntimeError(f"File_Reader: no data rows left in '{cfg['file_path']}' after skip/footer/comment filtering.")
        try:
            df = _spark().read.option("mode", "PERMISSIVE").json(_spark().sparkContext.parallelize(lines))
        except Exception as exc:
            raise RuntimeError(f"File_Reader: failed parsing JSON file '{cfg['file_path']}': {exc}") from exc

    if "_corrupt_record" in df.columns:
        bad = df.filter(F.col("_corrupt_record").isNotNull()).count()
        if bad > 0:
            raise ValueError(
                f"File_Reader: {bad} malformed JSON record(s) found in '{cfg['file_path']}'."
            )
        df = df.drop("_corrupt_record")
    return df


def _read_fixed_width(cfg):
    """Fixed-width parser, driven by `colspecs`: a list of
    (name, start, length) tuples, 0-indexed from the start of the line.
    """
    if not cfg["colspecs"]:
        raise ValueError("File_Reader: file_type='fixed_width' requires 'colspecs' "
                          "(a list of (name, start, length) tuples).")

    lines = _raw_lines(
        cfg["file_path"], cfg["encoding"], cfg["skip_rows"], cfg["footer_rows"], cfg["comment_prefix"]
    )
    if not lines:
        raise RuntimeError(f"File_Reader: no data rows left in '{cfg['file_path']}' after skip/footer/comment filtering.")

    raw_df = _spark().createDataFrame([(ln,) for ln in lines], ["value"])
    select_exprs = [
        F.substring(F.col("value"), start + 1, length).alias(name)
        for name, start, length in cfg["colspecs"]
    ]
    return raw_df.select(*select_exprs)


_PARSERS = {
    "csv": _read_csv,
    "json": _read_json,
    "fixed_width": _read_fixed_width,
}


# --------------------------------------------------------------------------- #
#  Standardisation - every parser funnels through this before returning
# --------------------------------------------------------------------------- #
def _apply_column_names(df, column_names):
    if not column_names:
        return df
    if len(column_names) != len(df.columns):
        raise ValueError(
            f"File_Reader: column_names has {len(column_names)} entries but the file has "
            f"{len(df.columns)} columns {df.columns}."
        )
    return df.toDF(*column_names)


def _apply_column_mapping(df, column_mapping):
    if not column_mapping:
        return df
    missing = [c for c in column_mapping if c not in df.columns]
    if missing:
        raise ValueError(
            f"File_Reader: column_mapping references column(s) not found in the file: {missing}. "
            f"Available columns: {df.columns}."
        )
    for raw_name, clean_name in column_mapping.items():
        df = df.withColumnRenamed(raw_name, clean_name)
    return df


def _dedupe_columns(df):
    """Suffix repeated column names (_2, _3, ...) so the DataFrame always has
    a unique column set, the same way a spreadsheet import would handle it."""
    seen = {}
    new_names = []
    changed = False
    for name in df.columns:
        seen[name] = seen.get(name, 0) + 1
        if seen[name] == 1:
            new_names.append(name)
        else:
            changed = True
            new_names.append(f"{name}_{seen[name]}")
    if changed:
        print(f"File_Reader: duplicate column names found, disambiguated to {new_names}")
        df = df.toDF(*new_names)
    return df


def _normalise_nulls(df, null_values):
    if not null_values:
        return df
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(
            col,
            F.when(F.trim(F.col(col)).isin(null_values), None).otherwise(F.col(col)),
        )
    return df


def _trim(df, trim_spaces):
    if not trim_spaces:
        return df
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))
    return df


def _apply_dtypes(df, dtype_mapping):
    if not dtype_mapping:
        return df
    missing = [c for c in dtype_mapping if c not in df.columns]
    if missing:
        raise ValueError(
            f"File_Reader: dtype_mapping references column(s) not found in the file: {missing}. "
            f"Available columns: {df.columns}."
        )
    for col, target_type in dtype_mapping.items():
        try:
            df = df.withColumn(col, F.col(col).cast(target_type))
        except Exception as exc:
            raise ValueError(
                f"File_Reader: could not cast column '{col}' to '{target_type}': {exc}"
            ) from exc
    return df


def _standardise(df, cfg):
    df = _apply_column_names(df, cfg["column_names"])
    df = _apply_column_mapping(df, cfg["column_mapping"])
    df = _dedupe_columns(df)
    df = _normalise_nulls(df, cfg["null_values"])
    df = _trim(df, cfg["trim_spaces"])
    df = _apply_dtypes(df, cfg["dtype_mapping"])
    return df


# --------------------------------------------------------------------------- #
#  Public entry point
# --------------------------------------------------------------------------- #
def read_file(config):
    """Read a file into a clean, standardised Spark DataFrame.

    Determines the parser from `config["file_type"]` (or the file extension
    if that's not set), runs it, then applies the common standardisation
    pass (column naming/mapping, duplicate-column handling, null
    normalisation, trimming, dtype casting) so every caller - regardless of
    source format - gets back a DataFrame shaped the same way a table would
    be. Prints the same progress lines Table-to-Table checks already print
    to, so a file-based run reads exactly like a table-based one on screen.
    """
    cfg = _resolve_config(config)
    label = cfg["label"]

    print(f"Reading {label} file: {cfg['file_path']} ({cfg['file_type']}) ...")
    _check_file_exists(cfg["file_path"])

    parser = _PARSERS.get(cfg["file_type"])
    if parser is None:
        raise ValueError(
            f"File_Reader: unsupported file_type '{cfg['file_type']}'. "
            "Use one of: csv, txt, json, pipe, tab, fixed_width."
        )

    print("Applying parser...")
    df = parser(cfg)

    print("Creating DataFrame...")
    df = _standardise(df, cfg)

    row_count = df.count()
    if row_count == 0:
        raise RuntimeError(f"File_Reader: '{cfg['file_path']}' produced 0 rows after parsing.")

    print(f"Rows loaded: {row_count}")
    print("Columns mapped successfully")
    return df


class FileReader:
    """Thin, dotted-access wrapper over `read_file()` - `FileReader.read_file(config)` -
    kept as a convenience alias; the parsing logic itself lives in the plain
    module-level functions above, same pattern as the PascalCase aliases at
    the bottom of Data_Validator.py.
    """
    read_file = staticmethod(read_file)


__all__ = [
    "read_file",
    "FileReader",
]
