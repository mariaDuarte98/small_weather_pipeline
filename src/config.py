import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("VC_API_KEY")
OUTPUT_PARQUET = "data/weather_partitioned"


CITIES = [
    {"name": "Lisbon", "lat": 38.7169, "lon": -9.1399},
    {"name": "London", "lat": 51.5074, "lon": -0.1278}, 
    {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917}, 
    {"name": "Ottawa", "lat": 45.4215, "lon": -75.6998},
    {"name": "Canberra", "lat": -35.2809, "lon": 149.1300},
    {"name": "Phoenix", "lat": 33.4484, "lon": -112.0740},
    {"name": "Kuwait City", "lat": 29.3759, "lon": 47.9774},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"name": "Jakarta", "lat": -6.2088, "lon": 106.8456},
    {"name": "Norilsk", "lat": 69.3558, "lon": 88.1893},
    {"name": "Fairbanks", "lat": 64.8378, "lon": -147.7164},
    {"name": "Miami", "lat": 25.7617, "lon": -80.1918},
    {"name": "Cairns", "lat": -16.9186, "lon": 145.7781}
]

