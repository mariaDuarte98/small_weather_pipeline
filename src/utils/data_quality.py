import logging

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"datetime", "city", "date", "year", "month", "tempmax", "tempmin", "temp"}
NOT_NULL_COLUMNS = ["datetime", "city", "date", "year", "month"]


def validate_schema(df: pd.DataFrame) -> bool:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        logger.warning(f"Data quality: missing columns {missing}")
        return False
    return True


def validate_no_nulls(df: pd.DataFrame, cols: list[str] = NOT_NULL_COLUMNS) -> bool:
    null_counts = df[cols].isnull().sum()
    nulls_found = null_counts[null_counts > 0]
    if not nulls_found.empty:
        logger.warning(f"Data quality: null values found:\n{nulls_found}")
        return False
    return True


def validate_row_count(df: pd.DataFrame, min_rows: int = 1) -> bool:
    if len(df) < min_rows:
        logger.warning(f"Data quality: expected at least {min_rows} rows, got {len(df)}")
        return False
    return True


def run_quality_checks(df: pd.DataFrame) -> bool:
    """Run all checks. Returns True only if all pass."""
    if df is None or df.empty:
        logger.warning("Data quality: DataFrame is empty.")
        return False
    checks = [
        validate_schema(df),
        validate_no_nulls(df),
        validate_row_count(df),
    ]
    passed = all(checks)
    if passed:
        logger.info("Data quality: all checks passed.")
    else:
        logger.warning("Data quality: one or more checks failed.")
    return passed
