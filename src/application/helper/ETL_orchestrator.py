
import logging

from application.helper.last_run_time import LastRunTime
from infrastructure.external_services.spark.init import init_spark


logging.basicConfig(level=logging.INFO)


class ETLOrchestrator:
    def __init__(self, loaders):
        self.loaders = loaders
        self.spark = init_spark()
        self.last_run_time = LastRunTime(spark=self.spark, root=self.root)

    def execute_all(self):
        try:
            for element in self.loaders:
                last_run_time = self.last_run_time.get(element.root, element.root_child_name)
                element.execute(last_run_time)
            logging.info("All raw data have been successfully transformed to processed data.")
        except Exception as e:
            logging.error(f"Error triggering transformation of raw to processed data: {e}")
