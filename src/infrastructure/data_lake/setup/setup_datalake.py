import os
import boto3
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

s3_endpoint = os.getenv("S3_ENDPOINT")
# Create an S3 client with a specified endpoint URL
s3 = boto3.client("s3", endpoint_url=s3_endpoint)

# Define the name of the bucket to be created
bucket_name = os.getenv("BUCKET_NAME")

# Create the S3 bucket
s3.create_bucket(Bucket=bucket_name)
s3.put_bucket_encryption(
    Bucket=bucket_name,
    ServerSideEncryptionConfiguration={
        'Rules': [
            {
                'ApplyServerSideEncryptionByDefault': {
                    'SSEAlgorithm': 'AES256'
                }
            },
        ]
    }
)
print(f"Bucket {bucket_name} created with server-side encryption")

# List of folders to be created within the bucket
folders = [
    "raw/users/",
    "raw/order_items/",
    "raw/orders/",
    "raw/products/",
    "raw/reviews/",
    "raw/interactions/",
    "processed/users/",
    "processed/order_items/",
    "processed/orders/",
    "processed/products/",
    "processed/reviews/",
    "processed/interactions/",
    "analytics/recommendations/",
    "analytics/sentiment_analysis/"
]

# Create each folder in the bucket by adding a placeholder file
for folder in folders:
    s3.put_object(Bucket=bucket_name, Key=(folder + "placeholder.txt"))
    print(f"Folder {folder} created successfully")
