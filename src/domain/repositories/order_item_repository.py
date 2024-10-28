from abc import ABC, abstractmethod

class OrderItemRepository(ABC):
    """
    Abstract class for order item repository.
    """

    @abstractmethod
    def get_all_order_items(self, last_run_time) -> list[dict]:
        pass
