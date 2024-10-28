

from domain.repositories.order_item_repository import OrderItemRepository
from infrastructure.database.make_query import make_query


class OrderItemRepositoryImpl(OrderItemRepository):
    def __init__(self):
        self.query_executor = make_query()

    def get_all_order_items(self, last_run_time) -> list[dict]:
        try:
            query = f"""SELECT * FROM "OrderItem" WHERE updatedAt > {last_run_time};"""
            order_items = self.query_executor.execute(query)
            return order_items
        except Exception as e:
            print(f"Error getting all order items: {e}")
            return []
