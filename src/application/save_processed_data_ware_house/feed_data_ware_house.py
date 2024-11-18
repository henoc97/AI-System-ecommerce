import logging
from prefect import flow, task

from infrastructure.external_services.spark.init import init_spark
from infrastructure.external_services.spark.spark_s3_utils import SparkS3Utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = init_spark()
class FeedDataWarehouse:
    
    def __init__(self):
        self.sparkS3Utils = SparkS3Utils(spark)
        
    @task
    def extract(self, processed_key: str, last_run_time):
        logger.info(f"Extracting data with processed_key: {processed_key}")
        try:
            data = self.sparkS3Utils.download_parquets(processed_key, last_run_time)
            logger.info(f"Extracted data: {data}")
            return data
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise
    
    
    @task
    def load(self):
        pass
    
    @flow(name='load processed data to data_warehouse')
    def run_pipeline(self):
        extract_data = self.extract()
        load_data = self.load(extract_data)