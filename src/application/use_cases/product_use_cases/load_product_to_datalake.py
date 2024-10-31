from application.services.product_service import ProductServices
from application.transform_data_to_datalake_raw.feed_datalake_raw_pipeline import FeedRawPipeline
from infrastructure.repositories_impl.product_repository_impl import ProductRepositoryImpl


class LoadProductToDatalake:
    def __init__(self):
        product_repository = ProductRepositoryImpl()
        self.feed_raw = FeedRawPipeline()
        self.product_services = ProductServices(product_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.product_services.get_all_products
            self.feed_raw.run_pipeline(get_data, 'raw/products', last_run_time)
        except Exception as e:
            print(f"Error loading products to datalake: {e}")
            
            

