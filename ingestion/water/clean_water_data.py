import pandas as pd
import json
import os
import logging
from ingest_water import fetch_water
import datetime

# Setup logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR,"water_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def extract_price(text: str):
    try:
        return float(text.split()[0])
    except:
        return None

def clean_water_tariff(json_path: str) -> pd.DataFrame:
    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    print(raw)

    df = pd.DataFrame(raw)

    # Create Column
    df['residential_price_clean'] = df['residential_price'].apply(extract_price)
    df['business_price_clean'] = df['business_price'].apply(extract_price)
    df["ingested_at"] = pd.Timestamp.now()
    df["source"] = "mwa"

    df_clean = df[[
        "residential_range", "residential_price_clean",
        "business_range", "business_price_clean",
        "ingested_at", "source"
    ]]

    return df_clean

if __name__ == "__main__":
    clean_water_tariff(f'data/raw/water_tariff_2025-05-15.json')
