import os
import logging
import pandas as pd
import psycopg2
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

def load_parquet_to_postgres(parquet_path: str):
    df = pd.read_parquet(parquet_path)

    # 🛡️ กรอง NaT (Not a Time) ออกจาก price_date
    df = df.dropna(subset=["price_date"])

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
        logging.info(f"Loaded {len(df)} rows from {parquet_path} into fuel_prices")

    except Exception as e:
        logging.error(f"Failed to load data to PostgreSQL: {e}")
        raise


if __name__ == '__main__':
    parquet_path = "data/processed/bangchak_prices_2025-05-15.parquet"
    load_parquet_to_postgres(parquet_path)
