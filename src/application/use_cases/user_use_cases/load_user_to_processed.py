import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.transform_raw_to_processed.transform_raw_processed_pipeline import TransformRawProcessedPipeline
from application.transform_raw_to_processed.transform_user import transform_user


class LoadUserToProcessed:
    def __init__(self):
        self.raw_to_processed = TransformRawProcessedPipeline()
    
    """
    Load the order items to the processed layer
    """
    def execute(self, last_run_time):
        raw_key = 'raw/users'
        processed_key = 'processed/users'
        transform_func = transform_user
        return self.raw_to_processed.run_pipeline(raw_key, processed_key, transform_func, last_run_time)