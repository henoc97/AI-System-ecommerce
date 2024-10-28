import os

bucket_base_url = os.getenv("BUCKET_BASE_URL")
def upload_processed_data(df, processed_key):
    try:
        print("DataFrame reçu pour upload :")
        print(df.head())
        
        bucket_url = f"{bucket_base_url}{processed_key}"
        
        # Load file to S3
        df.write.format("parquet").partitionBy("year", "month").save(bucket_url, mode="append")
                
        print(f"Processed data uploaded to {processed_key}")
    except Exception as e:
        print(f"Error uploading processed data: {e}")

