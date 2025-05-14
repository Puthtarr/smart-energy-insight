import os
import json
import time

import requests
import logging
import pandas as pd
import datetime
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

if __name__ == "__main__":
    date_str = datetime.date.today().isoformat()
    print(date_str)
    for i in range(3):
        try:
            fetch_water()
            print(f'Fetch water data Success')
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)

    for i in range(3):
        try:
            clean_water_df = clean_water_tariff(f'data/raw/water_tariff_{date_str}.json')
            print(f'Clean water data Success')
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)

    for i in range(3):
        try:
            file_path = save_as_parquet(clean_water_df, date_str)
            print(f"Save Fuel as Parquet Success")
            print(clean_water_df.columns)
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)

    bucket = "energy-data"
    object_path = f"water/{os.path.basename(file_path)}"
    for i in range(3):
        try:
            upload_to_minio(file_path, bucket_name=bucket, object_name=object_path)
            print(f'Upload {file_path} to Minio Success')
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)

    for i in range(3):
        try:
            load_water_to_postgres(file_path)
            print(f'Load Water Parquet {file_path} to Postgres Success')
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)