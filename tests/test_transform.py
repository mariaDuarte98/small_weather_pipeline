from unittest.mock import patch

import pandas as pd
import pytest

from src.transform import (
    compute_daily_anomalies,
    compute_rolling_avg,
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


class TestComputeDailyAnomalies:

    def test_adds_temp_anomaly_column(self):
        result = compute_daily_anomalies(_sample_df())
        assert "temp_anomaly" in result.columns

    def test_city_mean_anomaly_sums_to_zero(self):
        result = compute_daily_anomalies(_sample_df())
        for city in result["city"].unique():
            total = result[result["city"] == city]["temp_anomaly"].sum()
            assert abs(total) < 1e-9

    def test_does_not_mutate_input(self):
        df = _sample_df()
        before = df.copy()
        compute_daily_anomalies(df)
        pd.testing.assert_frame_equal(df, before)


class TestComputeRollingAvg:

    def test_adds_rolling_column(self):
        result = compute_rolling_avg(_sample_df())
        assert "temp_rolling_7d" in result.columns

    def test_rolling_avg_matches_single_row(self):
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

    def test_does_not_mutate_input(self):
        df = _sample_df()
        before = df.copy()
        compute_rolling_avg(df)
        pd.testing.assert_frame_equal(df, before)


class TestTransformWeather:

    @pytest.mark.parametrize("invalid_input", [None, pd.DataFrame()])
    def test_returns_empty_on_invalid_input(self, invalid_input):
        result = transform_weather(invalid_input)
        assert result.empty

    def test_adds_both_columns(self):
        result = transform_weather(_sample_df())
        assert "temp_anomaly" in result.columns
        assert "temp_rolling_7d" in result.columns


class TestSaveTransformed:

    def test_does_not_write_on_empty_df(self):
        with patch("src.transform.pq.write_to_dataset") as mock_write:
            save_transformed(pd.DataFrame())
            mock_write.assert_not_called()

    def test_writes_with_correct_partition_columns(self):
        with patch("src.transform.pq.write_to_dataset") as mock_write:
            save_transformed(_sample_df())
            mock_write.assert_called_once()
            assert mock_write.call_args.kwargs["partition_cols"] == ["city", "year", "month"]
