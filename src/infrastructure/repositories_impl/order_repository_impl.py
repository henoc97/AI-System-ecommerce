

from domain.repositories.order_repository import OrderRepository
from infrastructure.database.make_query import make_query


class OrderRepositoryImpl(OrderRepository):
    def __init__(self) -> None:
        self.query_executor = make_query()
        
    def get_all_orders(self, last_run_time) -> list[dict]:
        try:
            query = f"""SELECT * FROM "Order" WHERE updatedAt > {last_run_time};"""
            orders = self.query_executor.execute(query)
            return orders
        except Exception as e:
            print(f"Error getting all orders: {e}")
            return []
