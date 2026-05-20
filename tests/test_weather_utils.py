import pandas as pd

from src.utils.weather_utils import normalize_visual_crossing_data


class TestNormalizeVisualCrossingData:

    def test_adds_city_column(self):
        df = pd.DataFrame({"datetime": ["2024-01-01"]})
        result = normalize_visual_crossing_data(df, "Lisbon")
        assert result["city"].iloc[0] == "Lisbon"

    def test_adds_date_as_datetime(self):
        df = pd.DataFrame({"datetime": ["2024-01-15"]})
        result = normalize_visual_crossing_data(df, "Tokyo")
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_adds_year_and_month(self):
        df = pd.DataFrame({"datetime": ["2024-03-15"]})
        result = normalize_visual_crossing_data(df, "London")
        assert result["year"].iloc[0] == 2024
        assert result["month"].iloc[0] == 3

    def test_preserves_existing_columns(self):
        df = pd.DataFrame({"datetime": ["2024-01-01"], "temp": [22.5]})
        result = normalize_visual_crossing_data(df, "Miami")
        assert "temp" in result.columns

    def test_multiple_rows(self):
        df = pd.DataFrame({"datetime": ["2024-01-01", "2024-02-15", "2024-12-31"]})
        result = normalize_visual_crossing_data(df, "Ottawa")
        assert list(result["month"]) == [1, 2, 12]
        assert list(result["year"]) == [2024, 2024, 2024]
