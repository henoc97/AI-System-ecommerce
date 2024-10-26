

from application.transform_raw_to_processed.transform_pipeline import transform_raw_to_processed_pipeline
from application.transform_raw_to_processed.transform_order_item import transform_order_item


class LoadOrderItemToProcessed:
    def __init__(self):
        pass
    
    """
    Load the order items to the processed layer
    """
    def execute(self):
        raw_key = 'raw/order_items'
        processed_key = 'processed/order_items'
        transform_func = transform_order_item
        return transform_raw_to_processed_pipeline(raw_key, processed_key, transform_func)