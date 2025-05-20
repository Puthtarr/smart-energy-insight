# Smart Energy Insight (Local Edition)

A local, open-source data pipeline project for tracking and analyzing utility prices in Thailand, including:

- 💧 Water tariffs (MWA)
- ⚡ Electricity tariffs (MEA)
- ⛽ Fuel prices (Bangchak)

## Features

- ✅ Data Ingestion (Scraping + API)
- ✅ Data Cleaning with Pandas
- ✅ Data Storage as Parquet files
- ✅ Upload to MinIO (S3-compatible)
- ✅ Load into PostgreSQL
- ✅ Orchestration with Apache Airflow (optional)

## Architecture

```
            +-----------------+
            | Data Sources    |
            | (Web/API)       |
            +--------+--------+
                     |
                     v
     +---------------+---------------+
     | Ingestion (Python Scripts)   |
     +---------------+---------------+
                     |
                     v
     +---------------+---------------+
     | Data Cleaning with Pandas     |
     +---------------+---------------+
                     |
                     v
     +-------------------------------+
     | Save as Parquet               |
     +-------------------------------+
     | Upload to MinIO               |
     | Load to PostgreSQL            |
     +-------------------------------+
                     |
                     v
     +-------------------------------+
     | Visualization (Looker Studio)*|
     +-------------------------------+

```

## Project Structure

```
smart-energy-insight/
├── ingestion/
│   ├── fuel/
│   ├── water/
│   └── electric/
├── airflow/                # Optional: Airflow setup
├── .env                   # Environment variables
├── requirements.txt
└── README.md
```

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Setup MinIO and PostgreSQL via docker-compose
docker compose up -d
```

## Author

Piyaphat Putthasangwan