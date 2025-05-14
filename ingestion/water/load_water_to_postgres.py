import os
import pandas as pd
import psycopg2
import logging
from dotenv import load_dotenv

load_dotenv()

# Setup logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "water_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_water_to_postgres(parquet_path: str):
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
                INSERT INTO water_tariff (
                    residential_range,
                    residential_price,
                    business_range,
                    business_price,
                    ingested_at,
                    source
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ''',
                (
                    row["residential_range"],
                    row["residential_price_clean"],
                    row["business_range"],
                    row["business_price_clean"],
                    row["ingested_at"],
                    row["source"]
                )
            )

        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Loaded data from {parquet_path} into water_tariff table")

    except Exception as e:
        logging.error(f"Failed to load water data to PostgreSQL: {e}")
        raise

# Test แบบ manual:
if __name__ == "__main__":
    test_path = "data/processed/water_tariff_2025-05-08.parquet"
    load_water_to_postgres(test_path)