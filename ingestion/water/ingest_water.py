import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging

# Setup logging
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR,"water_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_water():
    url = "https://www.mwa.co.th/services/users-should-know/users-service-rate/service-rate/"
    print('get url request')
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "html.parser") # res.content = RAW html ที่ดึงจากหน้าเว็บ

    table = soup.find("table")
    rows = table.find_all("tr")[1:]  #skip Header

    data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) == 4: # table has 4 column and loop check every row that has 4 col
            data.append({
                "residential_range":cols[0].text.strip(),
                "residential_price":cols[1].text.strip(),
                "business_range":cols[2].text.strip(),
                "business_price":cols[3].text.strip()
            })

    # save to JSON
    date_str = datetime.today().strftime("%Y-%m-%d")
    output_dir = os.path.join("data", "raw")
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"water_tariff_{date_str}.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"✅ Saved MWA water tariff to: {out_path}")
    print(out_path)
    return out_path

if __name__ == "__main__":
    fetch_water()
