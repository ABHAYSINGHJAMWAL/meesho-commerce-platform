"""
Data quality checks for PySpark DataFrames.

Why this exists:
dbt tests run after SQL transformations.
PySpark jobs need equivalent checks in Python.
Same thinking — different layer.

Every check returns (passed, message).
Job fails fast if critical check fails.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


def check_not_empty(df: DataFrame, table_name: str) -> Tuple[bool, str]:
    count = df.count()
    if count == 0:
        return False, f"{table_name}: empty — 0 rows"
    return True, f"{table_name}: {count:,} rows OK"


def check_no_nulls(
    df: DataFrame,
    columns: List[str],
    table_name: str
) -> Tuple[bool, str]:
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            return False, f"{table_name}.{col}: {null_count} nulls found"
    return True, f"{table_name}: no nulls in {columns}"


def check_no_duplicates(
    df: DataFrame,
    key_columns: List[str],
    table_name: str
) -> Tuple[bool, str]:
    total = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    dupes = total - distinct
    if dupes > 0:
        return False, f"{table_name}: {dupes} duplicate keys on {key_columns}"
    return True, f"{table_name}: no duplicates on {key_columns}"


def check_value_range(
    df: DataFrame,
    column: str,
    min_val: float,
    max_val: float,
    table_name: str
) -> Tuple[bool, str]:
    out_of_range = df.filter(
        (F.col(column) < min_val) | (F.col(column) > max_val)
    ).count()
    if out_of_range > 0:
        return False, f"{table_name}.{column}: {out_of_range} values outside [{min_val}, {max_val}]"
    return True, f"{table_name}.{column}: all values in range"


def run_all_checks(
    df: DataFrame,
    table_name: str,
    required_columns: List[str] = None,
    key_columns: List[str] = None,
    amount_column: str = None
) -> bool:
    """
    Run standard suite of checks.
    Returns True if all pass, raises on critical failure.
    """
    checks = []

    checks.append(check_not_empty(df, table_name))

    if required_columns:
        checks.append(check_no_nulls(df, required_columns, table_name))

    if key_columns:
        checks.append(check_no_duplicates(df, key_columns, table_name))

    if amount_column:
        checks.append(check_value_range(df, amount_column, 0, 999999, table_name))

    all_passed = True
    for passed, message in checks:
        if passed:
            logger.info(f"CHECK PASSED: {message}")
        else:
            logger.error(f"CHECK FAILED: {message}")
            all_passed = False

    return all_passed