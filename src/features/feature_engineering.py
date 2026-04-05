import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def build_energy_features(
    input_path: str = 'data/02_intermediate/merged_data.parquet',
    output_path: str = 'data/03_features/model_ready.parquet',
    forecast_horizon: int = 24 
) -> str:
    """
    Ingests merged weather/energy data, imputes missing values, and engineers 
    temporal, lag, and weather-based features.
    
    Args:
        input_path: Path to the intermediate merged parquet.
        output_path: Path to save the model-ready dataset.
        forecast_horizon: How many hours ahead we are predicting (sets the minimum safe lag).
    """
    logging.info(f"Loading merged dataset from {input_path}")
    df = pd.read_parquet(input_path)
    
    logging.info("Imputing missing values using time-based interpolation...")
    df = df.set_index('timestamp')
    df['demand_mw'] = df['demand_mw'].interpolate(method='time')
    df['temperature_f'] = df['temperature_f'].interpolate(method='time')
    df = df.reset_index()

    logging.info("Extracting temporal features...")
    df['hour'] = df['timestamp'].dt.hour.astype(np.int8)
    df['day_of_week'] = df['timestamp'].dt.dayofweek.astype(np.int8)
    df['month'] = df['timestamp'].dt.month.astype(np.int8)
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(np.int8)

    logging.info("Engineering non-linear temperature features...")
    df['temp_squared'] = (df['temperature_f'] ** 2).astype(np.float32)
    
    df['temp_deviation_65'] = abs(df['temperature_f'] - 65).astype(np.float32)

    lag_hours = [0, 24, 144]
    logging.info(f"Generating lag features based on a {forecast_horizon}-hour horizon...")
    
    for lag in lag_hours:
        actual_lag = forecast_horizon + lag
        df[f'demand_lag_{actual_lag}h'] = df['demand_mw'].shift(actual_lag).astype(np.float32)

    logging.info("Generating rolling temperature statistics...")
    safe_temp_series = df['temperature_f'].shift(forecast_horizon)
    df['temp_rolling_mean_24h'] = safe_temp_series.rolling(window=24).mean().astype(np.float32)

    logging.info("Dropping initial rows with NaNs caused by lag shifts...")
    initial_rows = len(df)
    df = df.dropna().reset_index(drop=True)
    logging.info(f"Dropped {initial_rows - len(df)} rows. Final dataset size: {len(df)} rows.")

    out_dir = Path(output_path).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logging.info(f"Feature engineering complete. Data saved to {output_path}")
    
    return output_path

if __name__ == "__main__":
    build_energy_features()