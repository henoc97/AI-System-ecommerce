import os

bucket_base_url = os.getenv("BUCKET_BASE_URL")

def upload_file_to_s3(data, raw_key):
    try:
        path_key = f"{bucket_base_url}{raw_key}"
        
        data.write.format("parquet").save(path_key, mode="append")
        
        print(f"Data saved successfully: \n{data.printSchema()}")
    except Exception as e:
        print(f"Error uploading data to {raw_key} : {e}")

