

from domain.repositories.order_item_repository import OrderItemRepository
from infrastructure.database.make_query import MakeQuery


class OrderItemRepositoryImpl(OrderItemRepository):
    def __init__(self):
        self.query_executor = MakeQuery()

    def get_all_order_items(self, last_run_time) -> list[dict]:
        try:
            query = 'SELECT * FROM "OrderItem" WHERE "updatedAt" > %s;'
            params = (last_run_time,)
            order_items = self.query_executor.execute(query, params=params)
            return order_items
        except Exception as e:
            print(f"Error getting all order items: {e}")
            return []
