CREATE TABLE IF NOT EXISTS water_tariff (
    id SERIAL PRIMARY KEY,
    residential_range TEXT,
    residential_price FLOAT,
    business_range TEXT,
    business_price FLOAT,
    ingested_at TIMESTAMP,
    source TEXT
);
