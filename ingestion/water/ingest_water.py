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
    out_dir = "data/raw"
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f"water_tariff_{date_str}.json")

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Fetch Water tarif and Saved Success to {output_path}")
    return output_path

if __name__ == "__main__":
    fetch_water()
