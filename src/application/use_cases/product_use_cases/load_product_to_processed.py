import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')


from application.transform_raw_to_processed.transform_raw_processed_pipeline import TransformRawProcessedPipeline
from application.transform_raw_to_processed.transform_product import transform_product


class LoadProductToProcessed:
    def __init__(self):
        self.raw_to_processed = TransformRawProcessedPipeline()
    
    """
    Load the products to the processed layer
    """
    def execute(self, last_run_time):
        raw_key = 'raw/products'
        processed_key = 'processed/products'
        transform_func = transform_product
        return self.raw_to_processed.run_pipeline(raw_key, processed_key, transform_func, last_run_time)