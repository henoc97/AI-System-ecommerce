import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.transform_raw_to_processed.transform_raw_processed_pipeline import TransformRawProcessedPipeline
from application.transform_raw_to_processed.transform_review import transform_review


class LoadReviewToProcessed:
    def __init__(self):
        self.raw_to_processed = TransformRawProcessedPipeline()
    
    """
    Load the reviews to the processed layer
    """
    def execute(self, last_run_time):
        raw_key = 'raw/reviews'
        processed_key = 'processed/reviews'
        transform_func = transform_review
        return self.raw_to_processed.run_pipeline(raw_key, processed_key, transform_func, last_run_time)