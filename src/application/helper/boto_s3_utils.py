from datetime import datetime
import json
import boto3
import os


class BotoS3Utils:
    
    def __init__(self):
        self.s3_endpoint = os.getenv("S3_ENDPOINT")
        self.bucket_name = os.getenv("BUCKET_NAME")
        self.s3_client = boto3.client("s3", endpoint_url = self.s3_endpoint)
        
    def upload(self, content, s3_key):
        """
            s3_key : path
        """
        try:
            print(f'content : {content}')
            print(f's3_key : {s3_key}')
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=content)
            
            print(f"File metadata uploaded to {s3_key}")
        except Exception as e:
            print(f"Erreur lors de l'upload du fichier : {e}")
            
    def download_json(self, s3_key):
        """
            s3_key : path
            return json
            Do result.get('key') to get the value.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            result = json.load(response['Body'])
            return result
        except Exception as e:
            print(f"Erreur lors de la récupération du fichier : {e}")
            return None
