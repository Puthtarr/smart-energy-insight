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

from ingest_water import fetch_water
from clean_water_data import clean_water_tariff, extract_price
from water_datalake_uploader import save_as_parquet
from ingestion.common.datalake_uploader import *
from load_water_to_postgres import load_water_to_postgres

load_dotenv()

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "water_ingestion.log"),
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
        json_path = retry(lambda: fetch_water())
        logging.info("Fetched water tariff data")

        # 2. Clean
        df_clean = retry(lambda: clean_water_tariff(json_path))
        logging.info("Cleaned water tariff data")

        # 3. Save as Parquet
        parquet_path = retry(lambda: save_as_parquet(df_clean, date_str, prefix="water"))
        logging.info(f"Saved parquet: {parquet_path}")

        # 4. Upload to MinIO
        object_name = f"water/{os.path.basename(parquet_path)}"
        retry(lambda: upload_to_minio(parquet_path, "energy-data", object_name))
        logging.info("Uploaded to MinIO")

        # 5. Load to PostgreSQL
        retry(lambda: load_water_to_postgres(parquet_path))
        logging.info("Loaded to PostgreSQL")

        logging.info("Water pipeline complete.")

    except Exception as e:
        logging.error(f"Water pipeline failed: {e}")
        raise