import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.transform_raw_to_processed.transform_order import transform_order
from application.transform_raw_to_processed.transform_pipeline import transform_raw_to_processed_pipeline


class LoadOrderToProcessed:
    def __init__(self):
        pass
    
    """
    Load the order to the processed layer
    """
    def execute(self, last_run_time):
        raw_key = 'raw/orders/orders.csv'
        processed_key = 'processed/orders/orders.csv'
        transform_func = transform_order
        return transform_raw_to_processed_pipeline(raw_key, processed_key, transform_func, last_run_time)