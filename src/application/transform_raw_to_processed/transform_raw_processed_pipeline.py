from prefect import flow, task
import logging
from infrastructure.external_services.spark.init import init_spark
from infrastructure.external_services.spark.spark_s3_utils import SparkS3Utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = init_spark()

class TransformRawProcessedPipeline:
    def __init__(self):
        self.sparkS3Utils = SparkS3Utils(spark)

    @task
    def extract(self, raw_key: str, last_run_time):
        logger.info(f"Extracting data with raw_key: {raw_key}")
        try:
            data = self.sparkS3Utils.download_parquets(raw_key, last_run_time)
            logger.info(f"Extracted data: {data}")
            return data
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise

    @task
    def transform(self, fnc, df):
        logger.info("Transforming data.")
        try:
            transformed_data = fnc(df)
            logger.info("Data transformed.")
            return transformed_data
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise

    @task
    def load(self, df, processed_key):
        logger.info(f"Loading data to processed_key: {processed_key}")
        try:
            self.sparkS3Utils.upload_processed_data(df, processed_key)
            logger.info("Data loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    @flow(name="Transform Raw to Processed Pipeline")
    def run_pipeline(self, raw_key, processed_key, transform_function, last_run_time):
        try:
            logger.info("Starting pipeline")
            raw_data = self.extract(raw_key, last_run_time)
            print(f"raw data : {raw_data}")
            transformed_data = self.transform(transform_function, raw_data)
            self.load(transformed_data, processed_key)
            logger.info("Pipeline finished")
        except Exception as e:
            logger.error('Error processing: Transform Raw to Processed Pipeline')
            raise
