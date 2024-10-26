

from domain.repositories.order_repository import OrderRepository
from infrastructure.database.make_query import make_query


class OrderRepositoryImpl(OrderRepository):
    def __init__(self) -> None:
        self.query_executor = make_query()
        
    def get_all_orders(self) -> list[dict]:
        try:
            query = """SELECT * FROM "Order";"""
            orders = self.query_executor.execute(query)
            return orders
        except Exception as e:
            print(f"Error getting all orders: {e}")
            return []
