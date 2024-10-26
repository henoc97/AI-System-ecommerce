
from application.helper.to_csv import preprocess_data
from application.services.order_service import OrderServices
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.repositories_impl.order_repository_impl import OrderRepositoryImpl


class LoadOrderToDatalake:
    def __init__(self):
        order_repository = OrderRepositoryImpl()
        self.order_services = OrderServices(order_repository)
        
    def execute(self):
        try:
            orders = self.order_services.get_all_orders()
            output_file = 'src/infrastructure/data_lake/raw/orders/orders.csv'
            cols = ['id', 'userId', 'shopId', 'status', 'totalAmount', 'paymentId', 'trackingNumber', 'shippingMethod', 'createdAt',     'updatedAt']
            preprocess_data(orders, cols, output_file)
            upload_file_to_s3(output_file, 'raw/orders')
        except Exception as e:
            print(f"Error loading orders to datalake: {e}")
            