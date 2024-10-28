
from application.helper.to_csv import preprocess_data
from application.services.order_service import OrderServices
from application.transform_data_to_datalake_raw.transform_pipeline import extr_load_data_to_datalake_pipeline
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.repositories_impl.order_repository_impl import OrderRepositoryImpl


class LoadOrderToDatalake:
    def __init__(self):
        order_repository = OrderRepositoryImpl()
        self.order_services = OrderServices(order_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.order_services.get_all_orders
            extr_load_data_to_datalake_pipeline(get_data, 'raw/orders', last_run_time)
        except Exception as e:
            print(f"Error loading orders to datalake: {e}")
            