from prefect import flow, task
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from infrastructure.data_lake.setup.download_raw_data import download_raw_data
from infrastructure.data_lake.setup.upload_processed_data import upload_processed_data

@task
def extract(raw_key: str, last_run_time):
    logger.info(f"Extracting data with raw_key: {raw_key}")
    try:
        data = download_raw_data(raw_key, last_run_time)
        logger.info(f"Extracted data: {data}")
        return data
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

@task
def transform(fnc, df):
    logger.info(f"Transforming data: {df}")
    try:
        transformed_data = fnc(df)
        logger.info(f"Transformed data: {transformed_data}")
        return transformed_data
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

@task
def load(df, processed_key):
    logger.info(f"Loading data to processed_key: {processed_key}")
    try:
        result = upload_processed_data(df, processed_key)
        logger.info(f"Load result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

@flow(name="Transform Raw to Processed Pipeline")
def transform_raw_to_processed_pipeline(raw_key, processed_key, transform_function, last_run_time):
    logger.info("Starting pipeline")
    raw_data = extract(raw_key, last_run_time)
    transformed_data = transform(transform_function, raw_data)
    load(transformed_data, processed_key)
    logger.info("Pipeline finished")
