from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from prefect import task, flow

from application.transform_raw_to_processed.transform_raw_processed_pipeline import TransformRawProcessedPipeline
from infrastructure.external_services.spark.init import init_spark

# 1. Initialisation de l'environnement Spark
spark = init_spark()

# 2. Extraction des données
@task
def extract_data():
    # Exemple : charger un DataFrame depuis un fichier CSV
    df = spark.read.parquet("s3a://ecommerce-datalake/raw/users")
    print("Data received from ecommerce-datalake:")
    df.show()  # Utilisez df.show() sans f-string
    return df

# 3. Transformation des données
@task
def transform_data(df):
    # Exemple de transformation : ajouter une colonne et filtrer des lignes
    transformed_df = df.filter(col("id") > 10)
    return transformed_df

# 4. Chargement des données
@task
def load_data(df):
    # Exemple : écrire le DataFrame transformé dans un fichier Parquet
    df.write.parquet("s3a://ecommerce-datalake/transformed/users")

# 5. Création d'un flux Prefect pour orchestrer le pipeline
@flow(name="ETL Pipeline")  # Décorateur de flux
def etl_pipeline():
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)

# Exécution du flux
if __name__ == "__main__":
    # etl_pipeline()  # Appel direct du flux
    def tr():
        pass
    process = TransformRawProcessedPipeline()
    process.run_pipeline('raw/users', 'transformed1/users', tr, "2000-10-10 00:00:00")
