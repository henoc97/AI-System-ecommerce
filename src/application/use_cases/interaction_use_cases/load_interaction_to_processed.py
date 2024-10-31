import sys

from infrastructure.external_services.spark.init import init_spark


sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.transform_raw_to_processed.transform_raw_processed_pipeline import TransformRawProcessedPipeline
from application.transform_raw_to_processed.transform_interaction import transform_interaction


class LoadInteractionToProcessed:
    def __init__(self):
        self.raw_to_processed = TransformRawProcessedPipeline()
    
    """
    Load the interactions to the processed layer
    """
    def execute(self, last_run_time):      
        raw_key = 'raw/interactions'
        processed_key = 'processed/interactions'
        transform_func = transform_interaction
        spark = init_spark()
        return self.raw_to_processed.run_pipeline(raw_key, processed_key, transform_func, last_run_time)
