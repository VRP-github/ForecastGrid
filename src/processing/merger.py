import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def normalize_ts(df: pd.DataFrame) -> pd.DataFrame:
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    if df['timestamp'].dt.tz is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    else:
        df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
    return df



def validate_and_merge(energy_path: str, weather_path: str, output_path: str) -> pd.DataFrame:
    logging.info("Loading raw parquet files.")
    energy_df = pd.read_parquet(energy_path)
    weather_df = pd.read_parquet(weather_path)
    # energy_df['timestamp'] = pd.to_datetime(energy_df['timestamp'])
    # weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
    energy_df = normalize_ts(energy_df)
    weather_df = normalize_ts(weather_df)

    logging.info("Merging datasets on timestamp.")
    merged_df = pd.merge(energy_df, weather_df, on='timestamp', how='outer')
    merged_df = merged_df.sort_values('timestamp').reset_index(drop=True)

    if not merged_df['timestamp'].is_unique:
        duplicates = merged_df.duplicated(subset=['timestamp']).sum()
        logging.warning(f"Found {duplicates} duplicate timestamps. Keeping the first occurrence.")
        merged_df = merged_df.drop_duplicates(subset=['timestamp'], keep='first')

    full_time_range = pd.date_range(start=merged_df['timestamp'].min(),
                                    end=merged_df['timestamp'].max(),
                                    freq='h')

    missing_hours = len(full_time_range) - len(merged_df)
    if missing_hours > 0:
        logging.warning(
            f"Hourly continuity gap detected! Missing {missing_hours} hours. Reindexing to create NaN rows for imputation.")
        merged_df = merged_df.set_index('timestamp').reindex(full_time_range).reset_index()
        merged_df = merged_df.rename(columns={'index': 'timestamp'})  
    else:
        logging.info("Hourly continuity check passed perfectly.")

    null_rates = merged_df.isnull().mean() * 100
    logging.info(f"Missing value rates (%): \n{null_rates.to_string()}")

    out_dir = Path(output_path).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    logging.info(f"Saving validated dataset to {output_path}.")
    merged_df.to_parquet(output_path, index=False)
    return  merged_df

if __name__ =="__main__":
    validate_and_merge(
        energy_path='data/01_raw/energy.parquet',
        weather_path='data/01_raw/weather.parquet',
        output_path = 'data/02_intermediate/merged_data.parquet'
    )
    pass