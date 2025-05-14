-- block step
CREATE TABLE electric_block_tariff (
    range TEXT,
    unit TEXT,
    price_clean FLOAT,
    ingested_at TIMESTAMP,
    source TEXT
);

-- TOU tariff
CREATE TABLE electric_tou_tariff (
    voltage_range TEXT,
    on_peak FLOAT,
    off_peak FLOAT,
    service_fee FLOAT,
    ingested_at TIMESTAMP,
    source TEXT
);
