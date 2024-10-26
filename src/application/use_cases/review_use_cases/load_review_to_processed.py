

from application.transform_raw_to_processed.transform_pipeline import transform_raw_to_processed_pipeline
from application.transform_raw_to_processed.transform_review import transform_review


class LoadReviewToProcessed:
    def __init__(self):
        pass
    
    """
    Load the reviews to the processed layer
    """
    def execute(self):
        raw_key = 'raw/reviews'
        processed_key = 'processed/reviews'
        transform_func = transform_review
        return transform_raw_to_processed_pipeline(raw_key, processed_key, transform_func)