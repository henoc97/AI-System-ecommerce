
import logging


logging.basicConfig(level=logging.INFO)


class ETLOrchestrator:
    def __init__(self, loaders):
        self.loaders = loaders

    def execute_all(self):
        try:
            for loader in self.loaders:
                loader.execute()
            logging.info("All raw data have been successfully transformed to processed data.")
        except Exception as e:
            logging.error(f"Error triggering transformation of raw to processed data: {e}")
