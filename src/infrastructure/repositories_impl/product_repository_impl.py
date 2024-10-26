from domain.repositories.product_repository import ProductRepository
from infrastructure.database.make_query import make_query

class ProductRepositoryImpl(ProductRepository):
    """
    Implementation of the ProductRepository interface.
    """
    def __init__(self) -> None:
        self.query_executor = make_query()
    
    def get_all_products(self) -> list[dict]:
        try:
            query = """SELECT * FROM "Product";"""
            result = self.query_executor.execute(query)
            return result
        except Exception as e:
            print(f"Error getting all products: {e}")
            return []
