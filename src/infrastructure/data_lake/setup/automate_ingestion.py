from datetime import datetime
import boto3
import os

def upload_file_to_s3(local_file_path, s3_folder):
    bucket_name = "ecommerce-datalake"
    # Initialize the S3 client with a specific endpoint URL
    s3_client = boto3.client("s3", endpoint_url="http://localhost:4566")
    
    # Enable versioning on the bucket
    try:
        s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        print("Bucket versioning enabled")
    except Exception as e:
        print(f"Erreur lors de l'activation du versioning : {e}")

    # Construct the S3 key using the folder and the file name
    s3_key = f"{s3_folder}/{os.path.basename(local_file_path)}"

    # Upload the file to S3 with metadata and public-read access
    try:
        s3_client.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=s3_key, ExtraArgs={
            "Metadata": {
                "collected_date": datetime.now().strftime("%Y-%m-%d"),
                "data_type": s3_folder,
                "Content-Type": "text/csv"
            },
            "ACL": "public-read"
        })
        print(f"File {local_file_path} uploaded with metadata to {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Erreur lors de l'upload du fichier : {e}")

base_dir = os.path.dirname(os.path.abspath(__file__))  # Répertoire du script
file_path = os.path.join(base_dir, '../data-exemples/jewelery.csv')

if os.path.isfile(file_path):
    upload_file_to_s3(file_path, "raw/products")
else:
    print(f"Le fichier {file_path} n'existe pas.")
