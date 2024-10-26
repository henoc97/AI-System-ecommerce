

from application.helper.to_csv import preprocess_data
from application.services.order_item_service import OrderItemServices
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.repositories_impl.order_item_repository_impl import OrderItemRepositoryImpl


class LoadOrderItemToDatalake:
    def __init__(self):
        order_item_repository = OrderItemRepositoryImpl()
        self.order_item_services = OrderItemServices(order_item_repository)
        
    def execute(self):
        try:
            order_items = self.order_item_services.get_all_order_items()
            output_file = 'src/infrastructure/data_lake/raw/order_items/order_items.csv'
            cols = ['id', 'orderId', 'productId', 'quantity', 'price', 'created_at']
            preprocess_data(order_items, cols, output_file)
            upload_file_to_s3(output_file, 'raw/order_items')
        except Exception as e:
            print(f"Error loading order items to datalake: {e}")
            