# ⚡ Smart Energy Insight (Local Edition)

A local-first energy data analytics project that collects, transforms, and visualizes energy usage and pricing in Thailand — all without relying on paid cloud services.

---

## 📌 Project Overview

**Smart Energy Insight** aims to provide insight into Thailand's household energy data such as electricity, fuel, and water pricing, by:
- Scraping energy-related data from public sources
- Transforming raw data using PySpark or Pandas
- Storing structured data in PostgreSQL
- Visualizing trends and consumption using Superset or Looker Studio

All components run locally using Docker.

---

## 🧱 Tech Stack

| Layer | Tool |
|-------|------|
| Data Ingestion | Python + Selenium |
| Data Processing | PySpark / Pandas |
| Storage | PostgreSQL, MinIO, DuckDB |
| Orchestration | Cron / Apache Airflow (Docker) |
| Dashboard | Apache Superset / Looker Studio |

---

## 🐳 Docker Setup

### 📁 Project Structure
```
smart-energy-insight/
├── ingestion/
├── transform/
├── storage/
├── dashboard/
├── docker-compose.yml
└── .env
```

### ▶️ Run with Docker
```bash
docker compose up -d
```

This will start:
- 🐘 PostgreSQL at `localhost:5432`
- 📦 MinIO at `localhost:9000` (web UI at `localhost:9001`)

---

## 🚀 Usage

1. Configure `.env` file with access keys
2. Run ingestion scripts to collect raw data
3. Clean/transform with PySpark or Pandas
4. Load into PostgreSQL or DuckDB
5. Visualize in Superset dashboard

---

## 📈 Sample Dashboard (Coming Soon)
- Electricity price trends
- Household usage breakdown
- Monthly cost estimation

---

## 🧪 Development Status

✅ Docker setup  
✅ Project structure  
🔜 Scraping scripts  
🔜 Transformation pipelines  
🔜 Dashboard views  

---

## 🙋‍♂️ Author

Piyaphat Putthasangwan — [@Puthtarr](https://github.com/Puthtarr)

---

## 📄 License

This project is licensed under the MIT License.