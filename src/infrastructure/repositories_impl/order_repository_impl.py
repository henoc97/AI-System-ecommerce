

from domain.repositories.order_repository import OrderRepository
from infrastructure.database.make_query import MakeQuery


class OrderRepositoryImpl(OrderRepository):
    def __init__(self) -> None:
        self.query_executor = MakeQuery()
        
    def get_all_orders(self, last_run_time) -> list[dict]:
        try:
            # query = f'SELECT * FROM "Order" WHERE "updatedAt" > \'{last_run_time}\';'
            query = 'SELECT * FROM "Order" WHERE "updatedAt" > \'2000-01-01 00:00:00\';'
            orders = self.query_executor.execute(query)
            return orders
        except Exception as e:
            print(f"Error getting all orders: {e}")
            return []
