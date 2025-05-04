import os
import json
import requests
import logging
import pandas as pd
import datetime
import psycopg2
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "fuel_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_bangchak_api():
    url = "https://apigw01.blueenergy.cloud/api/BangchakOilPrice"
    try:
        res = requests.get(url)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logging.error(f"❌ Failed to fetch API: {e}")
        raise

def clean_bangchak_data(data) -> pd.DataFrame:
    try:
        oil_list = json.loads(data.get("OilList", "[]"))
        df = pd.DataFrame(oil_list)
        df["price_today"] = df["PriceToday"].astype(float)
        df["oil_name"] = df["OilName"]
        df["price_date"] = pd.to_datetime(data.get("OilPriceDate", ""), dayfirst=True, errors="coerce")
        df["ingested_at"] = pd.Timestamp.now()
        df["source"] = "bangchak"
        cleaned_df = df[["oil_name", "price_today", "price_date", "ingested_at", "source"]]
        logging.info("✅ Cleaned Bangchak data")
        return cleaned_df
    except Exception as e:
        logging.error(f"❌ Cleaning error: {e}")
        raise

def save_as_parquet(df: pd.DataFrame, date_str: str) -> str:
    output_dir = "data/processed/"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"bangchak_prices_{date_str}.parquet")
    df.to_parquet(file_path, index=False)
    logging.info(f"📁 Saved Parquet to: {file_path}")
    return file_path

def upload_to_minio(file_path: str, bucket_name: str, object_name: str = None):
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"http{'s' if use_ssl else ''}://{minio_endpoint}",
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            verify=use_ssl
        )
        buckets = s3_client.list_buckets()
        if not any(b["Name"] == bucket_name for b in buckets.get("Buckets", [])):
            s3_client.create_bucket(Bucket=bucket_name)
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"☁️ Uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
        logging.error(f"❌ Upload to MinIO failed: {e}")
        raise

def load_parquet_to_postgres(parquet_path: str):
    df = pd.read_parquet(parquet_path)
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute(
                '''
                INSERT INTO fuel_prices (oil_name, price_today, price_date, ingested_at, source)
                VALUES (%s, %s, %s, %s, %s)
                ''',
                (
                    row["oil_name"],
                    row["price_today"],
                    row["price_date"],
                    row["ingested_at"],
                    row["source"]
                )
            )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"✅ Loaded to PostgreSQL from {parquet_path}")
    except Exception as e:
        logging.error(f"❌ Load to PostgreSQL failed: {e}")
        raise

if __name__ == '__main__':
    raw_data = fetch_bangchak_api()
    df_clean = clean_bangchak_data(raw_data)
    today = datetime.date.today().isoformat()
    parquet_path = save_as_parquet(df_clean, today)
    upload_to_minio(parquet_path, "energy-data", f"fuel/bangchak_{today}.parquet")
    load_parquet_to_postgres(parquet_path)