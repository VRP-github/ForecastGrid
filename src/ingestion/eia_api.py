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
