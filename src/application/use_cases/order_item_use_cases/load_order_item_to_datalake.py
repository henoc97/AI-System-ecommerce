

from application.services.order_item_service import OrderItemServices
from application.transform_data_to_datalake_raw.transform_pipeline import extr_load_data_to_datalake_pipeline
from infrastructure.repositories_impl.order_item_repository_impl import OrderItemRepositoryImpl


class LoadOrderItemToDatalake:
    def __init__(self):
        order_item_repository = OrderItemRepositoryImpl()
        self.order_item_services = OrderItemServices(order_item_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.order_item_services.get_all_order_items
            extr_load_data_to_datalake_pipeline(get_data, 'raw/order_items', last_run_time)
        except Exception as e:
            print(f"Error loading order items to datalake: {e}")
            