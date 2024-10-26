

from application.transform_raw_to_processed.transform_pipeline import transform_raw_to_processed_pipeline
from application.transform_raw_to_processed.transform_interaction import transform_interaction


class LoadInteractionToProcessed:
    def __init__(self):
        pass
    
    """
    Load the interactions to the processed layer
    """
    def execute(self):      
        raw_key = 'raw/interactions'
        processed_key = 'processed/interactions'
        transform_func = transform_interaction
        return transform_raw_to_processed_pipeline(raw_key, processed_key, transform_func)