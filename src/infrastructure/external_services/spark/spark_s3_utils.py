import os
from pyspark.sql.functions import col
from dotenv import load_dotenv


# Charger les variables d'environnement
load_dotenv()

class SparkS3Utils:
    def __init__(self, spark):
        self.spark = spark
        self.bucket_base_url = os.getenv("BUCKET_BASE_URL")
        self.bucket_name = os.getenv("BUCKET_NAME")

    def upload_raw_data(self, data, s3_key):
        """
            s3_key : path
        """
        try:
            path_key = f"{self.bucket_base_url}/{ s3_key }"
            
            # Afficher le schéma de la table avant la sauvegarde
            print("Saving data with schema:")
            data.printSchema()
            
            # Sauvegarder le DataFrame dans S3 au format Parquet
            data.write.format("parquet").save(path_key, mode="append")
            
            print(f"Data saved successfully to {path_key}")
        except Exception as e:
            print(f"Error uploading data to {path_key} : {e}")
            
    def upload_processed_data(self, df, processed_key):
        try:
            print("DataFrame reçu pour upload :")
            print(f"'{df.show()}'")
            
            bucket_url = f"{self.bucket_base_url}/{processed_key}"
            
            # Load file to S3
            df.write.format("parquet").partitionBy("year", "month").save(bucket_url, mode="append")
                    
            print(f"Processed data uploaded to {processed_key}")
        except Exception as e:
            print(f"Error uploading processed data: {e}")
            
    def download_parquets(self, s3_key, last_run_time):
        """
            s3_key : path
        """
        # Download the path data from S3
        try:
            path_key = f"{self.bucket_base_url}/{ s3_key }"
            print(f"PATH: '{path_key}'")
            df = self.spark.read.parquet(path_key).filter(col("updatedAt") > last_run_time)
            print(f"Path data downloaded from { s3_key }")
            print(f"'{df.show()}'")
            return df
        except Exception as e:
            print(f"Error downloading path data: {e}")
            return None


