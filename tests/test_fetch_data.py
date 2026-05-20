from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from src.fetch_data import fetch_historical_weather, fetch_weather, save_data

SAMPLE_DAY = {"datetime": "2024-01-01", "temp": 15.0, "humidity": 70.0}


class TestFetchWeather:

    def test_returns_empty_df_when_start_date_is_none(self):
        result = fetch_weather("Lisbon", None, None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_returns_df_with_city_column_on_success(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"days": [SAMPLE_DAY]}
        with patch("src.fetch_data.requests.get", return_value=mock_response):
            result = fetch_weather("Lisbon", datetime(2024, 1, 1), datetime(2024, 1, 2))
        assert not result.empty
        assert result["city"].iloc[0] == "Lisbon"

    def test_returns_empty_df_when_api_returns_no_days(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"days": []}
        with patch("src.fetch_data.requests.get", return_value=mock_response):
            result = fetch_weather("Lisbon", datetime(2024, 1, 1), datetime(2024, 1, 2))
        assert result.empty

    def test_returns_empty_df_on_request_exception(self):
        err = requests.exceptions.RequestException("timeout")
        with patch("src.fetch_data.requests.get", side_effect=err):
            result = fetch_weather("Lisbon", datetime(2024, 1, 1), datetime(2024, 1, 2))
        assert result.empty

    def test_deduplicates_on_datetime(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "days": [
                {"datetime": "2024-01-01", "temp": 15.0},
                {"datetime": "2024-01-01", "temp": 16.0},
            ]
        }
        with patch("src.fetch_data.requests.get", return_value=mock_response):
            result = fetch_weather("Lisbon", datetime(2024, 1, 1), datetime(2024, 1, 2))
        assert len(result) == 1

    def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("403")
        with patch("src.fetch_data.requests.get", return_value=mock_response):
            result = fetch_weather("Lisbon", datetime(2024, 1, 1), datetime(2024, 1, 2))
        assert result.empty


class TestFetchHistoricalWeather:

    def _sample_city_df(self, city: str) -> pd.DataFrame:
        return pd.DataFrame({
            "datetime": ["2024-01-01"],
            "city": [city],
            "date": pd.to_datetime(["2024-01-01"]),
            "year": [2024],
            "month": [1],
        })

    def test_returns_one_row_per_city_on_success(self):
        side = lambda c, *_: self._sample_city_df(c)  # noqa: E731
        with patch("src.fetch_data.fetch_weather", side_effect=side):
            result = fetch_historical_weather("2024-01-01", "2024-01-02")
        assert len(result) == 13  # 13 cities in config

    def test_skips_cities_with_empty_result(self):
        def side_effect(city, *_):
            return self._sample_city_df(city) if city == "Lisbon" else pd.DataFrame()

        with patch("src.fetch_data.fetch_weather", side_effect=side_effect):
            result = fetch_historical_weather("2024-01-01", "2024-01-02")
        assert len(result) == 1
        assert result["city"].iloc[0] == "Lisbon"

    def test_returns_empty_df_when_all_cities_fail(self):
        with patch("src.fetch_data.fetch_weather", return_value=pd.DataFrame()):
            result = fetch_historical_weather("2024-01-01", "2024-01-02")
        assert result.empty

    def test_result_has_no_index_gaps(self):
        side = lambda c, *_: self._sample_city_df(c)  # noqa: E731
        with patch("src.fetch_data.fetch_weather", side_effect=side):
            result = fetch_historical_weather("2024-01-01", "2024-01-02")
        assert list(result.index) == list(range(len(result)))


class TestSaveData:

    def test_does_not_write_on_empty_df(self):
        with patch("src.fetch_data.pq.write_to_dataset") as mock_write:
            save_data(pd.DataFrame())
            mock_write.assert_not_called()

    def test_does_not_write_on_none(self):
        with patch("src.fetch_data.pq.write_to_dataset") as mock_write:
            save_data(None)
            mock_write.assert_not_called()

    def _sample_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "datetime": ["2024-01-01"],
            "city": ["Lisbon"],
            "date": pd.to_datetime(["2024-01-01"]),
            "year": [2024],
            "month": [1],
        })

    def test_writes_with_correct_partition_columns(self):
        with patch("src.fetch_data.pq.write_to_dataset") as mock_write:
            save_data(self._sample_df())
        mock_write.assert_called_once()
        assert mock_write.call_args.kwargs["partition_cols"] == ["city", "year", "month"]

    def test_uses_overwrite_or_ignore_behavior(self):
        with patch("src.fetch_data.pq.write_to_dataset") as mock_write:
            save_data(self._sample_df())
        assert mock_write.call_args.kwargs["existing_data_behavior"] == "overwrite_or_ignore"
