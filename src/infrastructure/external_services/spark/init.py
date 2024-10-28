import os
from pyspark.sql import SparkSession

s3_endpoint = os.getenv("S3_ENDPOINT")
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

def init_spark():
    return SparkSession.builder \
        .appName("Transform Raw to Processed Pipeline") \
        .config("Spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()