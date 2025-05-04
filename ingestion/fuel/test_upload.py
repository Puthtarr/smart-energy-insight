from upload_to_minio import upload_to_minio

upload_to_minio(
    file_path="data/processed/bangchak_prices_2025-05-05.parquet",
    bucket_name="energy-data",
    object_name="fuel/bangchak_2025-05-05.parquet"
)
