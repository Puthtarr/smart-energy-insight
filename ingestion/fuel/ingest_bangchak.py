import requests
import os
import json
import datetime
import logging

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "fuel_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_bangchak_raw(date_str: str) -> str:
    url = "https://oil-price.bangchak.co.th/ApiOilPrice2/th"
    output_dir = "data/raw/"
    os.makedirs(output_dir, exist_ok=True)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        file_path = os.path.join(output_dir, f"bangchak_{date_str}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logging.info(f"Fetched and saved raw Bangchak data to: {file_path}")
        print(file_path)
        return file_path

    except Exception as e:
        logging.error(f"Failed to fetch Bangchak data: {e}")
        raise

if __name__ == "__main__":
    # Test
    date_str = datetime.date.today().isoformat()
    json_path = fetch_bangchak_raw(date_str)
    print(f"Saved to: {json_path}")
