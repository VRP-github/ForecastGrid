import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def fetch_eia_demand(grid_region: str, days_back: int = 7) -> pd.DataFrame:
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise ValueError("CRITICAL ERROR: EIA API key not found. Please set the EIA_API_KEY environment variable.")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    start_str = start_date.strftime("%Y-%m-%dT00")
    end_str = end_date.strftime("%Y-%m-%dT23")
    print(f"Fetching Energy Demand for {grid_region} from {start_str} to {end_str}.")
    url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    region_mapping = {"TEXAS": "TEX", "CALIFORNIA": "CAL", "MIDWEST": "MISO"}
    eia_region_code = region_mapping.get(grid_region.upper(), "TEX")

    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": eia_region_code,
        "facets[type][]": "D",
        "start": start_str,
        "end": end_str,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc"
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        records = data.get("response", {}).get("data", [])
        if not records:
            raise ValueError("API returned successfully, but no data was found.")

        df = pd.DataFrame(records)
        df = df[['period', 'value']]
        df = df.rename(columns={'period': 'timestamp', 'value': 'demand_mw'})
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['demand_mw'] = pd.to_numeric(df['demand_mw'])

        print(f"Successfully fetched {len(df)} hours of energy data.")
        safe_start = start_date.strftime('%Y-%m-%d')
        safe_end = end_date.strftime('%Y-%m-%d')
        df.to_parquet(f"data/01_raw/energy_{grid_region}_{safe_start}_to_{safe_end}.parquet", index=False)

        return df
    except requests.exceptions.RequestException as e:
        print(f"CRITICAL ERROR: Failed to fetch EIA data. Details: {e}")
        raise