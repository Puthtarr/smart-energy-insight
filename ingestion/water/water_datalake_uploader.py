import os
import logging
import datetime
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from pandas.core.interchange.dataframe_protocol import DataFrame
import requests
from clean_water_data import clean_water_tariff
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

def save_as_parquet(df: pd.DataFrame, date_str: str, prefix: str = 'water') -> str:
    output_dir = os.path.join('data', 'processed')
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{prefix}_tariff_{date_str}.parquet")

    try:
        df.to_parquet(file_path, index=False)
        logging.info(f'Saved parquet to {file_path}')
        return file_path

    except Exception as ex:
        logging.info(f'Failed to save parquet : {ex}')
        raise


# def upload_to_minio(file_path: str, bucket_name: str, object_name: str = None):
#     minio_endpoint = os.getenv("MINIO_ENDPOINT")
#     minio_access_key = os.getenv("MINIO_ACCESS_KEY")
#     minio_secret_key = os.getenv("MINIO_SECRET_KEY")
#     use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
#
#     if object_name is None:
#         object_name = os.path.basename(file_path)
#
#     try:
#         s3_client = boto3.client(
#             "s3",
#             endpoint_url=f"http{'s' if use_ssl else ''}://{minio_endpoint}",
#             aws_access_key_id=minio_access_key,
#             aws_secret_access_key=minio_secret_key,
#             verify=use_ssl
#         )
#
#         # Create bucket if not exists
#         buckets = s3_client.list_buckets()
#         if not any(b["Name"] == bucket_name for b in buckets.get("Buckets", [])):
#             s3_client.create_bucket(Bucket=bucket_name)
#
#         s3_client.upload_file(file_path, bucket_name, object_name)
#         logging.info(f"Uploaded {file_path} to MinIO bucket {bucket_name} as {object_name}")
#
#     except FileNotFoundError:
#         logging.error(f"File not found: {file_path}")
#         raise
#     except NoCredentialsError:
#         logging.error("Credentials not available for MinIO")
#         raise
#     except Exception as e:
#         logging.error(f"Failed to upload to MinIO: {e}")
#         raise

if __name__ == "__main__":
    date_str = datetime.date.today().isoformat()
    df_clean = clean_water_tariff('data/raw/water_tariff_2025-05-15.json')
    save_as_parquet(df_clean, date_str)



    file_path = "data/processed/water_tariff_2025-05-08.parquet"
    bucket = "energy-data"
    object_path = f"water/{os.path.basename(file_path)}"

    # upload_to_minio(file_path, bucket_name=bucket, object_name=object_path)