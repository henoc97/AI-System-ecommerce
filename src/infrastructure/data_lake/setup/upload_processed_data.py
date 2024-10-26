

import boto3
from io import StringIO

# Create an S3 client with a specified endpoint URL
s3_client = boto3.client("s3", endpoint_url="http://localhost:4566")

# Define the name of the bucket to be created
bucket_name = "ecommerce-datalake"

def upload_processed_data(df, processed_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_client.upload_fileobj(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=processed_key)
    print(f"Processed data uploaded to {processed_key}")
