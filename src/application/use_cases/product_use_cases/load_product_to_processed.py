

from application.transform_raw_to_processed.transform_pipeline import transform_raw_to_processed_pipeline
from application.transform_raw_to_processed.transform_product import transform_product


class LoadProductToProcessed:
    def __init__(self):
        pass
    
    """
    Load the products to the processed layer
    """
    def execute(self):
        raw_key = 'raw/products'
        processed_key = 'processed/products'
        transform_func = transform_product
        return transform_raw_to_processed_pipeline(raw_key, processed_key, transform_func)