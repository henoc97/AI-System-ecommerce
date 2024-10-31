from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv

from infrastructure.external_services.spark.spark_s3_utils import SparkS3Utils

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

def init_spark():
    s3_endpoint = os.getenv("S3_ENDPOINT")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Vérifier si les variables d'environnement sont correctement définies
    if not all([s3_endpoint, access_key, secret_key]):
        raise ValueError("Please ensure that S3_ENDPOINT, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY are set in the environment variables.")

    # Chemin vers le répertoire contenant les JARs
    jars_path = os.path.expanduser("~/jars")  # Utiliser ~ pour accéder au répertoire personnel
    hadoop_aws_jar = os.path.join(jars_path, "hadoop-aws-3.3.1.jar")
    aws_java_sdk_jar = os.path.join(jars_path, "aws-java-sdk-bundle-1.12.118.jar")

    spark = (
        SparkSession.builder
        .appName("Transform Raw to Processed Pipeline") \
        .config("spark.jars", f"{hadoop_aws_jar},{aws_java_sdk_jar}") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.hadoop.fs.s3a.committer.name", "directory") \
        .config("spark.hadoop.fs.s3a.committer.staging.enabled", "true") \
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    )
    
    spark.conf.set("fs.s3a.access.key", access_key)
    spark.conf.set("fs.s3a.secret.key", secret_key)
    spark.conf.set("fs.s3a.endpoint", s3_endpoint)

    # Ajouter implémentation explicitement dans la configuration Hadoop
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Définir le niveau de log si nécessaire
    # spark.sparkContext.setLogLevel("DEBUG")

    return spark

if __name__ == "__main__":
    spark = init_spark()
    utils = SparkS3Utils(spark=spark)
    
    df = utils.download_parquets("raw/users", "2024-10-04 00:00:00", spark)
    print('response : ')
    print(df.show())