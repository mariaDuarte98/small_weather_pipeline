import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("VC_API_KEY")
if not API_KEY:
    raise EnvironmentError("VC_API_KEY is not set. Add it to your .env file.")

OUTPUT_PARQUET = str(Path(__file__).resolve().parents[1] / "data" / "weather_partitioned")

CITIES = [
    "Lisbon",
    "London",
    "Tokyo",
    "Ottawa",
    "Canberra",
    "Phoenix",
    "Kuwait City",
    "Mumbai",
    "Jakarta",
    "Norilsk",
    "Fairbanks",
    "Miami",
    "Cairns",
]

