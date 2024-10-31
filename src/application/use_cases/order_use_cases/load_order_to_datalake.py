
from application.services.order_service import OrderServices
from application.transform_data_to_datalake_raw.feed_datalake_raw_pipeline import FeedRawPipeline
from infrastructure.repositories_impl.order_repository_impl import OrderRepositoryImpl


class LoadOrderToDatalake:
    def __init__(self):
        order_repository = OrderRepositoryImpl()
        self.feed_raw = FeedRawPipeline()
        self.order_services = OrderServices(order_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.order_services.get_all_orders
            self.feed_raw.run_pipeline(get_data, 'raw/orders', last_run_time)
        except Exception as e:
            print(f"Error loading orders to datalake: {e}")
            