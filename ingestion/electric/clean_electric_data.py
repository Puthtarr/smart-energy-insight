import logging

import pandas as pd
import json
import os

LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "electricity_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def extract_price(text):
    # "price": "2.3488 บาท" to 2.3488
    try:
        return float(text.strip().split()[0])
    except:
        return None

def clean_mea_tariff(json_path: str):
    with open(json_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

        # --- Clean block_step ---
        block_df = pd.DataFrame(raw["block_step"])
        block_df["price_clean"] = block_df["price"].apply(extract_price)
        block_df["ingested_at"] = pd.Timestamp.now()
        block_df["source"] = "mea"

        block_clean = block_df[["range", "unit", "price_clean", "ingested_at", "source"]]

        # --- Clean tou_tariff ---
        tou_df = pd.DataFrame(raw["tou_tariff"])
        tou_df["on_peak"] = tou_df["on_peak"].apply(extract_price)
        tou_df["off_peak"] = tou_df["off_peak"].apply(extract_price)
        tou_df["service_fee"] = tou_df["service_fee"].apply(extract_price)
        tou_df["ingested_at"] = pd.Timestamp.now()
        tou_df["source"] = "mea"

        tou_clean = tou_df[[
            "voltage_range", "on_peak", "off_peak", "service_fee", "ingested_at", "source"
        ]]

        logging.info(f"Cleaned MEA electricity tariff from {json_path}")
        return block_clean, tou_clean

# ทดสอบ:
if __name__ == "__main__":
    json_path = r"D:\Data Project\Portfolio Project\smart-energy-insight\ingestion\electric\data\raw\mea_tariff_2025-05-14.json"
    df_block, df_tou = clean_mea_tariff(json_path)
    print("Block Tariff:")
    print(df_block.head())
    print("TOU Tariff:")
    print(df_tou.head())