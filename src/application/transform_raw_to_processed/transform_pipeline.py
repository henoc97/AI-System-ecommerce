import pandas as pd
from prefect import flow, task, IntervalSchedule
from datetime import timedelta

from infrastructure.data_lake.setup.download_raw_data import download_raw_data
from infrastructure.data_lake.setup.upload_processed_data import upload_processed_data

@task
def extract(raw_key: str, processed_key: str):
    return download_raw_data(raw_key=raw_key, processed_key=processed_key)

@task
def transform(fnc: callable, df: pd.DataFrame):
    return fnc(df)

@task
def load(df: pd.DataFrame, processed_key: str):
    return upload_processed_data(df, processed_key)

# Define a recurring execution schedule
schedule = IntervalSchedule(interval=timedelta(days=1))

@flow(schedule=schedule)
def transform_raw_to_processed_pipeline(raw_key: str, processed_key: str, transform_function: callable):
    raw_data = extract(raw_key, processed_key)
    transformed_data = transform(transform_function, raw_data)
    load(transformed_data, processed_key)