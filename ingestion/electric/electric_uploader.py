import os
import datetime
import pandas as pd
from ingestion.electric.clean_electric_data import clean_mea_tariff
from ingestion.common.datalake_uploader import upload_to_minio

import logging
from dotenv import load_dotenv

# Load .env
load_dotenv()

# Setup Logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "electricity_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def save_as_parquet(df: pd.DataFrame, date_str: str, prefix: str) -> str:
    """
    Save cleaned electricity dataframe to processed folder as parquet
    """
    output_dir = os.path.join("ingestion", "electric", "data", "processed")
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, f"{prefix}_tariff_{date_str}.parquet")

    try:
        df.to_parquet(file_path, index=False)
        logging.info(f"Saved parquet: {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Failed to save parquet: {e}")
        raise

if __name__ == "__main__":
    # Define date
    date_str = datetime.date.today().isoformat()

    # Path to cleaned JSON
    json_path = f"data/raw/mea_tariff_{date_str}.json"

    # Clean
    df_block, df_tou = clean_mea_tariff(json_path)

    # Save as parquet
    path_block = save_as_parquet(df_block, date_str, prefix="electric_block")
    path_tou = save_as_parquet(df_tou, date_str, prefix="electric_tou")

    # Upload
    bucket_name = "energy-data"
    upload_to_minio(path_block, bucket_name, object_name=f"electricity/{os.path.basename(path_block)}")
    upload_to_minio(path_tou, bucket_name, object_name=f"electricity/{os.path.basename(path_tou)}")

    print("Upload complete")
