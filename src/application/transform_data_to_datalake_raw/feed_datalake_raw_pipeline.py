from prefect import flow, task
import logging

from infrastructure.external_services.spark.init import init_spark
from infrastructure.external_services.spark.spark_s3_utils import SparkS3Utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FeedRawPipeline:

    def __init__(self):
        self.spark = init_spark()
        self.sparkS3Utils = SparkS3Utils(self.spark)
        
    @task
    def extract(self, get_data, last_run_time):
        logger.info(f"Extracting data...")
        try:
            data = get_data(last_run_time)
            logger.info(f"Extracted data: {data}")
            return data
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise

    @task
    def load(self, data, raw_key):
        logger.info(f"Loading data to raw_key: {raw_key}")
        try:
            
            self.sparkS3Utils.upload_raw_data(data=data, s3_key=raw_key)
            logger.info(f"Load successed.")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    @flow(name="Extract and Load Data to Datalake raw Pipeline")
    def run_pipeline(self, get_data, raw_key, last_run_time):
        try:
            logger.info("Starting pipeline")
            raw_data = self.extract(get_data, last_run_time)
            
            if not raw_data:
                print("Aucune donnée à traiter. Le DataFrame ne peut pas être créé.")
                return []

            cleaned_data = [
                {k: ("" if v is None else v) for k, v in record.items()}
                for record in raw_data
            ]
            
            spark = init_spark()
            data = spark.createDataFrame(cleaned_data)
            
            data = data.fillna("")
            
            self.load(data=data, raw_key=raw_key)
            logger.info("Pipeline finished")
        except Exception as e:
            logger.error(f"Error in pipeline: {e}")
            raise
