import pandas as pd

from src.utils.data_quality import (
    run_quality_checks,
    validate_no_nulls,
    validate_row_count,
    validate_schema,
)


def _valid_df() -> pd.DataFrame:
    return pd.DataFrame({
        "datetime": ["2024-01-01"],
        "city": ["Lisbon"],
        "date": pd.to_datetime(["2024-01-01"]),
        "year": [2024],
        "month": [1],
        "tempmax": [20.0],
        "tempmin": [10.0],
        "temp": [15.0],
    })


class TestValidateSchema:

    def test_passes_with_all_required_columns(self):
        assert validate_schema(_valid_df()) is True

    def test_fails_when_column_missing(self):
        df = _valid_df().drop(columns=["tempmax"])
        assert validate_schema(df) is False

    def test_passes_with_extra_columns(self):
        df = _valid_df()
        df["extra"] = 1
        assert validate_schema(df) is True


class TestValidateNoNulls:

    def test_passes_when_no_nulls(self):
        assert validate_no_nulls(_valid_df()) is True

    def test_fails_when_key_column_has_null(self):
        df = _valid_df()
        df.loc[0, "city"] = None
        assert validate_no_nulls(df) is False


class TestValidateRowCount:

    def test_passes_with_rows(self):
        assert validate_row_count(_valid_df()) is True

    def test_fails_when_empty(self):
        assert validate_row_count(pd.DataFrame(), min_rows=1) is False

    def test_passes_exact_minimum(self):
        assert validate_row_count(_valid_df(), min_rows=1) is True


class TestRunQualityChecks:

    def test_passes_on_valid_df(self):
        assert run_quality_checks(_valid_df()) is True

    def test_fails_on_none(self):
        assert run_quality_checks(None) is False

    def test_fails_on_empty_df(self):
        assert run_quality_checks(pd.DataFrame()) is False

    def test_fails_when_column_missing(self):
        df = _valid_df().drop(columns=["tempmax"])
        assert run_quality_checks(df) is False

    def test_fails_when_null_in_required_col(self):
        df = _valid_df()
        df.loc[0, "city"] = None
        assert run_quality_checks(df) is False
