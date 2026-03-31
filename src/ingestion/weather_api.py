import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_weather_data(lat:float,lon:float, days_back: int = 7) -> pd.DataFrame:
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    print(f"Fetching weather data for Lat: {lat}, Lon: {lon} from {start_date} to {end_date}.")

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        'latitude': lat,
        'longitude': lon,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        'hourly': "temperature_2m",
        "timezone": "America/Chicago"
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame({
            "timestamp": pd.to_datetime(data["hourly"]["time"]),
            "temperature_f": data["hourly"]["temperature_2m"]
        })
        df['temperature_f'] = (df['temperature_f'] * 9/5) + 32
        print(f"Successfully fetched {len(df)} hours of weather data.")
        df.to_parquet(f"data/01_raw/weather_{start_date}_to_{end_date}.prequet", index=False)
        return df
    except requests.exceptions.RequestException as e:
        print(f"CRITICAL ERROR: Failed to fetch weather data. Details: {e}")
        raise