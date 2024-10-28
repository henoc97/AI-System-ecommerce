
from domain.repositories.product_repository import ProductRepository


class ProductServices:
    def __init__(self, product_repository: ProductRepository):
        self.product_repository = product_repository
        
    def get_all_products(self, last_run_time) -> list[dict]:
        return self.product_repository.get_all_products(last_run_time)