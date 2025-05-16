import os
import logging
from enum import verify

import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

# load ENV
load_dotenv()

# Setup logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, 'datalake_uploader.log'),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def upload_to_minio(file_path: str, bucket_name: str, object_name: str = None):
    # Check ENV
    required_env = ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_USE_SSL"]
    missing = [var for var in required_env if os.getenv(var) is None]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    use_ssl = os.getenv("MINIO_USE_SSL").lower() == "true"

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

        # Create Bucket if Not Exist
        buckets = s3_client.list_buckets()
        if not any(b["Name"] == bucket_name for b in buckets.get("Buckets", [])):
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Created new bucket: {bucket_name}")

        # Upload file
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"Uploaded {file_path} to bucket '{bucket_name}' as '{object_name}'")

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except NoCredentialsError:
        logging.error("MinIO credentials not found in environment")
        raise
    except ClientError as e:
        logging.error(f"MinIO ClientError: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to upload to MinIO: {e}")
        raise