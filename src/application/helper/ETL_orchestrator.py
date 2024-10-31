import logging

from application.helper.last_run_time import LastRunTime
from infrastructure.external_services.spark.init import init_spark


logging.basicConfig(level=logging.INFO)


class ETLOrchestrator:
    def __init__(self, loaders):
        self.loaders = loaders
        self.spark = init_spark()
        self.last_run_time = LastRunTime(spark=self.spark)

    def execute_all(self):
        try:
            for element in self.loaders:
                if 'root' in element:  # Vérification si 'root' est une clé dans le dictionnaire
                    last_run_time = self.last_run_time.get(element['root'], element['root_child_name'])
                    print(f"last_run_time : '{last_run_time}'")
                    element['loader'].execute(last_run_time)  # Accès à l'objet loader dans le dictionnaire
                    self.last_run_time.update(element['root'], element['root_child_name'])
                else:
                    logging.error(f"L'élément ne possède pas la clé 'root': {element}")
            logging.info("All raw data have been successfully transformed data.")
        except Exception as e:
            logging.error(f"Error triggering transformation of data: {e}")
