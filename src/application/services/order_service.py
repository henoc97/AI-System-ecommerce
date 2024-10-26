
from domain.repositories.order_repository import OrderRepository


class OrderServices:
    def __init__(self, order_repository: OrderRepository):
        self.order_repository = order_repository
        
    def get_all_orders(self) -> list[dict]:
        return self.order_repository.get_all_orders()
