
from abc import ABC, abstractmethod


class OrderRepository(ABC):
    """
    Abstract class for order repository.
    """

    @abstractmethod
    def get_all_orders(self, last_run_time) -> list[dict]:
        pass
