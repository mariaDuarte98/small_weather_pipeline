from unittest.mock import patch

import pandas as pd
import pytest

from src.transform import (
    compute_daily_anomalies,
    compute_rolling_avg,
    load_raw,
    save_transformed,
    transform_weather,
)


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "datetime": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
        "city": ["Lisbon", "Lisbon", "London", "London"],
        "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
        "temp": [15.0, 17.0, 5.0, 7.0],
        "year": [2024, 2024, 2024, 2024],
        "month": [1, 1, 1, 1],
    })


class TestLoadRaw:

    def test_returns_full_dataset(self):
        """Returns all rows from the raw Parquet dataset without filtering."""
        with patch("src.transform.pq.read_table") as mock_read:
            mock_read.return_value.to_pandas.return_value = _sample_df()
            result = load_raw()
        assert len(result) == len(_sample_df())

    def test_returns_empty_df_on_read_error(self):
        """Returns an empty DataFrame when the Parquet read raises an exception."""
        with patch("src.transform.pq.read_table", side_effect=Exception("not found")):
            result = load_raw()
        assert result.empty


class TestComputeDailyAnomalies:

    def test_adds_temp_anomaly_column(self):
        """Result contains a temp_anomaly column."""
        result = compute_daily_anomalies(_sample_df())
        assert "temp_anomaly" in result.columns

    def test_city_mean_anomaly_sums_to_zero(self):
        """Anomalies are deviations from the city mean, so they sum to zero per city."""
        result = compute_daily_anomalies(_sample_df())
        for city in result["city"].unique():
            total = result[result["city"] == city]["temp_anomaly"].sum()
            assert abs(total) < 1e-9

    def test_anomaly_values_are_correct(self):
        """Anomaly equals each row's temp minus its city mean across the dataset."""
        result = compute_daily_anomalies(_sample_df())
        # Lisbon: mean=(15+17)/2=16 → anomalies [-1, 1]
        lisbon = result[result["city"] == "Lisbon"].sort_values("date")["temp_anomaly"].tolist()
        assert lisbon == pytest.approx([-1.0, 1.0])
        # London: mean=(5+7)/2=6 → anomalies [-1, 1]
        london = result[result["city"] == "London"].sort_values("date")["temp_anomaly"].tolist()
        assert london == pytest.approx([-1.0, 1.0])

    def test_does_not_mutate_input(self):
        """Input DataFrame is unchanged; the function returns a copy."""
        df = _sample_df()
        before = df.copy()
        compute_daily_anomalies(df)
        pd.testing.assert_frame_equal(df, before)


class TestComputeRollingAvg:

    def test_adds_rolling_column(self):
        """Result contains a temp_rolling_7d column."""
        result = compute_rolling_avg(_sample_df())
        assert "temp_rolling_7d" in result.columns

    def test_rolling_avg_matches_single_row(self):
        """A single-row city has a rolling average equal to its own value."""
        df = pd.DataFrame({
            "city": ["Lisbon"],
            "date": pd.to_datetime(["2024-01-01"]),
            "temp": [10.0],
            "datetime": ["2024-01-01"],
            "year": [2024],
            "month": [1],
        })
        result = compute_rolling_avg(df)
        assert result["temp_rolling_7d"].iloc[0] == pytest.approx(10.0)

    def test_rolling_avg_values_are_correct(self):
        """Rolling average accumulates correctly as more rows fall within the window."""
        result = compute_rolling_avg(_sample_df())
        # Lisbon sorted by date: day1=15 → avg 15.0; day2=17 → avg (15+17)/2=16.0
        lisbon = result[result["city"] == "Lisbon"].sort_values("date")["temp_rolling_7d"].tolist()
        assert lisbon == pytest.approx([15.0, 16.0])
        # London sorted by date: day1=5 → avg 5.0; day2=7 → avg (5+7)/2=6.0
        london = result[result["city"] == "London"].sort_values("date")["temp_rolling_7d"].tolist()
        assert london == pytest.approx([5.0, 6.0])

    def test_does_not_mutate_input(self):
        """Input DataFrame is unchanged; the function returns a copy."""
        df = _sample_df()
        before = df.copy()
        compute_rolling_avg(df)
        pd.testing.assert_frame_equal(df, before)


class TestTransformWeather:

    @pytest.mark.parametrize("invalid_input", [None, pd.DataFrame()])
    def test_returns_empty_on_invalid_input(self, invalid_input):
        """Returns an empty DataFrame when given None or an empty DataFrame."""
        result = transform_weather(invalid_input)
        assert result.empty

    def test_adds_both_columns(self):
        """Applies both transforms and returns a DataFrame with both output columns."""
        result = transform_weather(_sample_df())
        assert "temp_anomaly" in result.columns
        assert "temp_rolling_7d" in result.columns


class TestSaveTransformed:

    def test_does_not_write_on_empty_df(self):
        """Skips writing to Parquet when given an empty DataFrame."""
        with patch("src.transform.pq.write_to_dataset") as mock_write:
            save_transformed(pd.DataFrame())
            mock_write.assert_not_called()

    def test_writes_with_correct_partition_columns(self):
        """Partitions output by city, year, and month."""
        with patch("src.transform.pq.write_to_dataset") as mock_write:
            save_transformed(_sample_df())
            mock_write.assert_called_once()
            assert mock_write.call_args.kwargs["partition_cols"] == ["city", "year", "month"]
