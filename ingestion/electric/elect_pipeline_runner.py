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

from ingest_elec import fetch_electric
from clean_electric_data import clean_mea_tariff
from electric_uploader import save_as_parquet
from load_electric_to_postgres import load_parquet_to_postgres
from ingestion.common.datalake_uploader import upload_to_minio

load_dotenv()

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "electricity_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    try:
        # 1. Ingest
        json_path = fetch_electric()
        logging.info("Fetched electricity tariff")

        # 2. Clean
        block_df, tou_df = clean_mea_tariff(json_path)
        logging.info("Cleaned electricity tariff data")

        # 3. Save as Parquet
        date_str = datetime.today().strftime("%Y-%m-%d")
        block_path = save_as_parquet(block_df, date_str, prefix="electric_block")
        tou_path = save_as_parquet(tou_df, date_str, prefix="electric_tou")

        # 4. Upload to MinIO
        upload_to_minio(block_path, bucket_name="energy-data", object_name=f"electricity/block/{os.path.basename(block_path)}")
        upload_to_minio(tou_path, bucket_name="energy-data", object_name=f"electricity/tou/{os.path.basename(tou_path)}")

        # 5. Load to PostgreSQL
        load_parquet_to_postgres(block_path, table_name='electric_block_tariff')
        load_parquet_to_postgres(tou_path, table_name='electric_tou_tariff')

        logging.info("Electric pipeline complete.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise