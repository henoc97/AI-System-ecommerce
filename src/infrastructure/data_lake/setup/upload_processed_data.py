import boto3
import pandas as pd
from io import StringIO

# Création d'un client S3
s3_client = boto3.client("s3", endpoint_url="http://localhost:4566")
bucket_name = "ecommerce-datalake"

def upload_processed_data(df, processed_key):
    try:
        # Vérifier le contenu du DataFrame
        print("DataFrame reçu pour upload :")
        print(df.head())  # Affiche les premières lignes du DataFrame pour vérification
        
        # Conversion du DataFrame en CSV dans un StringIO
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Réinitialisation du pointeur pour l'envoi
        csv_buffer.seek(0)
        
        # Affichage des 100 premiers caractères pour vérification
        print(f"Uploading to {processed_key} with data: {csv_buffer.getvalue()[:100]}")
        
        # Chargement du fichier sur S3
        s3_client.put_object(Bucket=bucket_name, Key=processed_key, Body=csv_buffer.getvalue())
        
        print(f"Processed data uploaded to {processed_key}")
    except Exception as e:
        print(f"Error uploading processed data: {e}")

# # Exemple d'utilisation
# df = pd.DataFrame({
#     "id": [1, 2, 3],
#     "name": ["Product 1", "Product 2", "Product 3"],
#     "price": [100, 200, 300]
# })

# upload_processed_data(df, "processed/products/data.csv")
