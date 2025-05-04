import os
import logging
import pandas as pd
import datetime
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ตรวจสอบ env ที่จำเป็น
required_env = ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_USE_SSL"]
missing = [var for var in required_env if os.getenv(var) is None]
if missing:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "fuel_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def save_as_parquet(df: pd.DataFrame, date_str: str) -> str:
    output_dir = "data/processed/"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"bangchak_prices_{date_str}.parquet")

    try:
        df.to_parquet(file_path, index=False)
        logging.info(f"Saved Parquet to: {file_path}")
        return file_path

    except Exception as e:
        logging.error(f"Failed to save Parquet: {e}")
        raise


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
        logging.info(f"Uploaded {file_path} to MinIO bucket '{bucket_name}' as '{object_name}'")

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except NoCredentialsError:
        logging.error("Credentials not available for MinIO")
        raise
    except Exception as e:
        logging.error(f"Failed to upload to MinIO: {e}")
        raise

