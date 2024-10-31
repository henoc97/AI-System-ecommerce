import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.transform_raw_to_processed.transform_order import transform_order
from application.transform_raw_to_processed.transform_raw_processed_pipeline import TransformRawProcessedPipeline


class LoadOrderToProcessed:
    def __init__(self):
        self.raw_to_processed = TransformRawProcessedPipeline()
    
    """
    Load the order to the processed layer
    """
    def execute(self, last_run_time):
        raw_key = 'raw/orders'
        processed_key = 'processed/orders'
        transform_func = transform_order
        return self.raw_to_processed.run_pipeline(raw_key, processed_key, transform_func, last_run_time)