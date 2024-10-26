from abc import ABC, abstractmethod

class ProductRepository(ABC):
    """
    Abstract class for product repository.
    """
    
    @abstractmethod
    def get_all_products(self) -> list[dict]:
        pass