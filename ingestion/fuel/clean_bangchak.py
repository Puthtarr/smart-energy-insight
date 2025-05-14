import pandas as pd
import json
import os
import logging

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "fuel_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def clean_bangchak_data(json_path: str) -> pd.DataFrame:
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list) and "OilList" in data[0]:
            oil_list_raw = data[0]["OilList"]  # ดึง string JSON
            oil_list = json.loads(oil_list_raw)  # แปลงเป็น list

            df = pd.DataFrame(oil_list)

            df["price_today"] = df["PriceToday"].astype(float)
            df["oil_name"] = df["OilName"]
            df["price_date"] = pd.to_datetime(data[0].get("OilPriceDate", ""), dayfirst=True, errors="coerce")
            df["ingested_at"] = pd.Timestamp.now()
            df["source"] = "bangchak"

            cleaned_df = df[["oil_name", "price_today", "price_date", "ingested_at", "source"]]

            logging.info(f"Cleaned Bangchak data from: {json_path}")
            return cleaned_df
        else:
            raise ValueError("NOT FOUND 'OilList' In JSON")

    except Exception as e:
        logging.error(f"Failed to clean data from {json_path}: {e}")
        raise

if __name__ == '__main__':
    df = clean_bangchak_data("data/raw/bangchak_2025-05-15.json")
    print(df.head())
