import os
import logging
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "electricity_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_parquet_to_postgres(parquet_path: str, table_name: str):
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
            columns = ", ".join(df.columns)
            values = ", ".join(["%s"] * len(df.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cur.execute(sql, tuple(row))

        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Loaded {len(df)} rows into {table_name}")
    except Exception as e:
        logging.error(f"Failed to load {parquet_path} into {table_name}: {e}")
        raise

if __name__ == "__main__":
    date_str = pd.Timestamp.today().strftime("%Y-%m-%d")

    path_block = f"ingestion/electric/data/processed/electric_block_tariff_{date_str}.parquet"
    path_tou = f"ingestion/electric/data/processed/electric_tou_tariff_{date_str}.parquet"

    load_parquet_to_postgres(path_block, table_name="electric_block_tariff")
    load_parquet_to_postgres(path_tou, table_name="electric_tou_tariff")

    print("Loaded both electric tariff tables into PostgreSQL.")
