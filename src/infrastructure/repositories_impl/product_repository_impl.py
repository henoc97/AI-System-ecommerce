from domain.repositories.product_repository import ProductRepository
from infrastructure.database.make_query import MakeQuery

class ProductRepositoryImpl(ProductRepository):
    """
    Implementation of the ProductRepository interface.
    """
    def __init__(self) -> None:
        self.query_executor = MakeQuery()
    
    def get_all_products(self, last_run_time) -> list[dict]:
        try:
            query = 'SELECT * FROM "Product" WHERE "updatedAt" > %s;'
            params = (last_run_time,)
            result = self.query_executor.execute(query, params=params)
            return result
        except Exception as e:
            print(f"Error getting all products: {e}")
            return []
