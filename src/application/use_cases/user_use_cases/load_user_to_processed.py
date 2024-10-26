

from application.transform_raw_to_processed.transform_pipeline import transform_raw_to_processed_pipeline
from application.transform_raw_to_processed.transform_user import transform_user


class LoadUserToProcessed:
    def __init__(self):
        pass
    
    """
    Load the order items to the processed layer
    """
    def execute(self):
        raw_key = 'raw/users'
        processed_key = 'processed/users'
        transform_func = transform_user
        return transform_raw_to_processed_pipeline(raw_key, processed_key, transform_func)