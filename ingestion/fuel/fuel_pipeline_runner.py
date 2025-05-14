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

if __name__ == '__main__':
    date_str = datetime.date.today().isoformat()
    print(date_str)
    for i in range(3):
        try:
            fetch_bangchak_raw(date_str)
            print(f'Fetch data Success')
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)

    for i in range(3):
        try:
            clean_df = clean_bangchak_data(f"data/raw/bangchak_{date_str}.json")
            print(f'Clean data Success')
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)

    for i in range(3):
        try:
            file_path = save_as_parquet(clean_df, date_str)
            print(f"Save Fuel as Parquet Success")
            print(clean_df.columns)
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)

    bucket = "energy-data"
    object_path = f"fuel/{os.path.basename(file_path)}"
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
            load_parquet_to_postgres(file_path)
            print(f'Load Parquet {file_path} to Postgres Success')
            break
        except Exception as ex:
            print(f'Error {ex}')
            time.sleep(10)









