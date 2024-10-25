import boto3

# Create an S3 client with a specified endpoint URL
s3 = boto3.client("s3", endpoint_url="http://localhost:4566")

# Define the name of the bucket to be created
bucket_name = "ecommerce-datalake"

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
    "raw/orders/",
    "raw/products/",
    "raw/interactions/",
    "processed/users/",
    "processed/orders/",
    "processed/products/",
    "processed/interactions/",
    "analytics/recommendations/",
    "analytics/sentiment_analysis/"
]

# Create each folder in the bucket by adding a placeholder file
for folder in folders:
    s3.put_object(Bucket=bucket_name, Key=(folder + "placeholder.txt"))
    print(f"Folder {folder} created successfully")