import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.transform_raw_to_processed.transform_raw_processed_pipeline import TransformRawProcessedPipeline
from application.transform_raw_to_processed.transform_order_item import transform_order_item


class LoadOrderItemToProcessed:
    def __init__(self):
        self.raw_to_processed = TransformRawProcessedPipeline()
    
    """
    Load the order items to the processed layer
    """
    def execute(self, last_run_time):
        raw_key = 'raw/order_items'
        processed_key = 'processed/order_items'
        transform_func = transform_order_item
        return self.raw_to_processed.run_pipeline(raw_key, processed_key, transform_func, last_run_time)