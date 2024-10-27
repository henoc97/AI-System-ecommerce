from prefect import flow, task

from infrastructure.data_lake.setup.download_raw_data import download_raw_data
from infrastructure.data_lake.setup.upload_processed_data import upload_processed_data

@task
def extract(raw_key: str, processed_key: str):
    print(f"Extracting data with raw_key: {raw_key} and processed_key: {processed_key}")
    data = download_raw_data(raw_key=raw_key, processed_key=processed_key)
    print(f"Extracted data: {data}")
    return data

@task
def transform(fnc, df):
    print(f"Transforming data: {df}")
    transformed_data = fnc(df)
    print(f"Transformed data: {transformed_data}")
    return transformed_data

@task
def load(df, processed_key):
    print(f"Loading data to processed_key: {processed_key}")
    result = upload_processed_data(df, processed_key)
    print(f"Load result: {result}")
    return result

# Define a recurring execution schedule
# schedule = IntervalSchedule(interval=timedelta(days=1))

# @flow(schedule=schedule)
@flow(name="Transform Raw to Processed Pipeline")
def transform_raw_to_processed_pipeline(raw_key, processed_key, transform_function):
    print("Starting pipeline")
    raw_data = extract(raw_key, processed_key)
    transformed_data = transform(transform_function, raw_data)
    load(transformed_data, processed_key)
    print("Pipeline finished")
