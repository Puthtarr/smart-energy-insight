import os
import logging
import pandas as pd
import datetime
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from clean_bangchak import clean_bangchak_data

# Load environment variables
load_dotenv()

# credential .env check
required_env = ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_USE_SSL"]
missing = [var for var in required_env if os.getenv(var) is None]
if missing:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

# Setup logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR,"water_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def save_as_parquet(df: pd.DataFrame, date_str: str, prefix: str = 'fuel') -> str:
    output_dir = os.path.join('data', 'processed')
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{prefix}_tariff_{date_str}.parquet")

    try:
        df.to_parquet(file_path, index=False)
        logging.info(f"Saved Parquet to: {file_path}")
        return file_path

    except Exception as e:
        logging.error(f"Failed to save Parquet: {e}")
        raise

if __name__ == "__main__":
    date_str = datetime.date.today().isoformat()
    df_clean = clean_bangchak_data('data/raw/bangchak_2025-05-15.json')
    save_as_parquet(df_clean, date_str)