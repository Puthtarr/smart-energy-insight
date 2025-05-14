import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import json
import logging

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "electricity_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_electric():
    url = "https://www.mea.or.th/our-services/tariff-calculation/other/D5xEaEwgU"
    print('Sending GET request...')
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')

    tables = list(soup.find_all("table"))
    print(f"Found {len(tables)} tables")

    data = {
        "block_step": [],
        "tou_tariff": []
    }

    # ตาราง 1.1 และ 1.2 → Block Step Tariff
    for table in tables[:2]:
        rows = table.find_all("tr")
        for row in rows:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) == 3 and "หน่วยละ" in cols[1]:
                data["block_step"].append({
                    "range": cols[0],
                    "unit": cols[1],
                    "price": cols[2]
                })
            elif len(cols) >= 2 and "ค่าบริการ" in cols[0]:
                data["block_step"].append({
                    "range": "service_fee",
                    "unit": "บาท/เดือน",
                    "price": cols[1]
                })

    # ตาราง 1.3 → TOU Tariff
    if len(tables) >= 3:
        for row in tables[2].find_all("tr")[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) == 4:
                data["tou_tariff"].append({
                    "voltage_range": cols[0],
                    "on_peak": cols[1],
                    "off_peak": cols[2],
                    "service_fee": cols[3]
                })

    # Save JSON
    date_str = datetime.today().strftime("%Y-%m-%d")
    out_dir = os.path.join("data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"mea_tariff_{date_str}.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved MEA electricity tariff to: {out_path}")
    logging.info(f"Fetched and saved MEA electricity tariff to: {out_path}")
    return out_path

# ทดสอบรันเดี่ยว
if __name__ == "__main__":
    fetch_electric()
