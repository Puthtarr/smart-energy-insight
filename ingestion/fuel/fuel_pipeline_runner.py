import os
import json
import time

import requests
import logging
import pandas as pd
from datetime import datetime
import psycopg2
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from ingest_bangchak import fetch_bangchak_raw
from clean_bangchak import clean_bangchak_data
from datalake_uploader import save_as_parquet
from ingestion.common.datalake_uploader import *
from load_fuel_to_postgres import load_parquet_to_postgres

load_dotenv()

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "fuel_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def retry(func, max_attempts=3, wait_sec=5):
    for i in range(max_attempts):
        try:
            return func()
        except Exception as ex:
            logging.warning(f"Retry {i+1}/{max_attempts} failed: {ex}")
            time.sleep(wait_sec)
    raise RuntimeError(f"Failed after {max_attempts} attempts")

if __name__ == "__main__":
    try:
        date_str = datetime.today().strftime("%Y-%m-%d")

        # 1. Fetch
        retry(lambda: fetch_bangchak_raw(date_str))
        logging.info("Fetch fuel data success")

        # 2. Clean
        json_path = f"data/raw/bangchak_{date_str}.json"
        clean_df = retry(lambda: clean_bangchak_data(json_path))
        logging.info("Cleaned fuel data")

        # 3. Save Parquet
        parquet_path = retry(lambda: save_as_parquet(clean_df, date_str, prefix="bangchak_prices"))
        logging.info(f"Saved parquet: {parquet_path}")

        # 4. Upload to MinIO
        object_name = f"fuel/{os.path.basename(parquet_path)}"
        retry(lambda: upload_to_minio(parquet_path, "energy-data", object_name))
        logging.info("Uploaded to MinIO")

        # 5. Load to PostgreSQL
        retry(lambda: load_parquet_to_postgres(parquet_path))
        logging.info("Loaded to PostgreSQL")

        logging.info("Fuel pipeline complete.")

    except Exception as e:
        logging.error(f"Fuel pipeline failed: {e}")
        raise








