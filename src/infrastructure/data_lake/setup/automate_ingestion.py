import datetime
import boto3
import os

def upload_file_to_s3(local_file_path, s3_folder):
    """
    Uploads a file to an S3 bucket with metadata and enables bucket versioning.

    Parameters:
    local_file_path (str): The local path to the file to be uploaded.
    s3_folder (str): The S3 folder where the file will be uploaded.
    """
    
    bucket_name = "ecommerce-datalake"
    # Initialize the S3 client with a specific endpoint URL
    s3_client = boto3.client("s3", endpoint_url="http://localhost:4566")
    
    # Enable versioning on the bucket
    s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
    print("Bucket versioning enabled")

    # Construct the S3 key using the folder and the file name
    s3_key = f"{s3_folder}/{os.path.basename(local_file_path)}"

    # Upload the file to S3 with metadata and public-read access
    s3_client.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=s3_key, ExtraArgs={
        "Metadata": {
            "collected_date": datetime.now().strftime("%Y-%m-%d"),
            "data_type": s3_folder,
            "Content-Type": "text/csv"
        },
        "ACL": "public-read"})  # Access control list setting
    print(f"File {local_file_path} uploaded with metadata to {bucket_name}/{s3_key}")

# Example usage of the function
upload_file_to_s3("../data-exemples/jewelery.csv", "raw/products")
