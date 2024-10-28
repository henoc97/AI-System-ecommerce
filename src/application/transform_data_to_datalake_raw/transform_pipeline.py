from prefect import flow, task
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.data_lake.setup.download_raw_data import download_raw_data
from infrastructure.data_lake.setup.upload_processed_data import upload_processed_data

@task
def extract(get_data, last_run_time):
    logger.info(f"Extracting data...")
    try:
        data = get_data(last_run_time)
        logger.info(f"Extracted data: {data}")
        return data
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

@task
def load(data, raw_key):
    logger.info(f"Loading data to raw_key: {raw_key}")
    try:
        upload_file_to_s3(data=data, raw_key=raw_key)
        logger.info(f"Load successed.")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

@flow(name="Extract and Load Data to Datalake/raw Pipeline")
def extr_load_data_to_datalake_pipeline(get_data, raw_key, last_run_time):
    logger.info("Starting pipeline")
    raw_data = extract(get_data, last_run_time)
    load(data=raw_data, raw_key=raw_key)
    logger.info("Pipeline finished")
