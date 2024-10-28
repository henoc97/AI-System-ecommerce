

from domain.repositories.order_item_repository import OrderItemRepository


class OrderItemServices:
    def __init__(self, order_item_repository: OrderItemRepository):
        self.order_item_repository = order_item_repository
        
    def get_all_order_items(self, last_run_time) -> list[dict]:
        return self.order_item_repository.get_all_order_items(last_run_time)
