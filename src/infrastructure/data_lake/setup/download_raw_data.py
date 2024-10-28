import os
from infrastructure.external_services.spark.init import init_spark

bucket_name = os.getenv("BUCKET_NAME")

bucket_base_url = os.getenv("BUCKET_BASE_URL")


def download_raw_data(raw_key, last_run_time):
    # Download the path data from S3
    try:
        spark = init_spark()
        
        path_key = f"{bucket_base_url}{raw_key}"
        
        df = spark.read.parquet(path_key).filter(f"updatedAt > '{last_run_time}'")
        print(f"Path data downloaded from {raw_key}")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error downloading path data: {e}")
        return None
