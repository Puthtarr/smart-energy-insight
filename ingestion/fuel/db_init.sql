-- db_init.sql

CREATE TABLE IF NOT EXISTS fuel_prices (
    id SERIAL PRIMARY KEY,
    oil_name TEXT NOT NULL,
    price_today NUMERIC(10, 2),
    price_date DATE,
    ingested_at TIMESTAMP,
    source TEXT
);