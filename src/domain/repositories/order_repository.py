
from abc import ABC, abstractmethod


class OrderRepository(ABC):
    """
    Abstract class for order repository.
    """

    @abstractmethod
    def get_all_orders(self) -> list[dict]:
        pass
