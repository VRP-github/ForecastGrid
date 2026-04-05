import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

        null_count = df['temperature_f'].isnull().sum()
        logging.info(f"Weather Data - Rows: {len(df)}, Nulls: {null_count}")
        logging.info(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")

        output_path = 'data/01_raw/weather.parquet'
        out_dir = Path(output_path).parent
        out_dir.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output_path, index=False)
        logging.info(f"Successfully saved to {output_path}")

        return df
    except requests.exceptions.RequestException as e:
        print(f"CRITICAL ERROR: Failed to fetch weather data. Details: {e}")
        raise

if __name__ == "__main__":
    fetch_weather_data(lat=41.8781, lon=-87.6298, days_back=7)
