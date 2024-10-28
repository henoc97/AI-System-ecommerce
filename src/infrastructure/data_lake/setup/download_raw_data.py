import boto3
import pandas as pd
from io import StringIO

# Initialize the S3 client with a specific endpoint URL
s3_client = boto3.client("s3", endpoint_url="http://localhost:4566")

bucket_name = "ecommerce-datalake"

def download_raw_data(path_key):
    # Download the path data from S3
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=path_key)
        path_data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(path_data))
        print(f"Path data downloaded from {path_key}")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error downloading path data: {e}")
        return None
