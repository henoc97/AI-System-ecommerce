

from domain.repositories.order_repository import OrderRepository
from infrastructure.database.make_query import MakeQuery


class OrderRepositoryImpl(OrderRepository):
    def __init__(self) -> None:
        self.query_executor = MakeQuery()
        
    def get_all_orders(self, last_run_time) -> list[dict]:
        try:
            query = 'SELECT * FROM "Order" WHERE "updatedAt" > %s;'
            params = (last_run_time,)
            orders = self.query_executor.execute(query, params=params)
            return orders
        except Exception as e:
            print(f"Error getting all orders: {e}")
            return []
