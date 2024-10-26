import os

from application.helper.to_csv import preprocess_data
from application.services.product_service import ProductServices
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.repositories_impl.product_repository_impl import ProductRepositoryImpl


class LoadProductToDatalake:
    def __init__(self):
        product_repository = ProductRepositoryImpl()
        self.product_services = ProductServices(product_repository)
        
    def execute(self):
        try:
            products = self.product_services.get_all_products()
            output_file = 'src/infrastructure/data_lake/raw/products/products.csv'
            cols = ['id', 'name', 'description', 'price', 'categoryId', 'stock', 'shopId', 'created_at', 'updated_at', 'vendorId']
            preprocess_data(products, cols, output_file)
            upload_file_to_s3(output_file, 'raw/products')
        except Exception as e:
            print(f"Error loading products to datalake: {e}")
            
            

