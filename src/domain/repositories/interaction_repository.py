

from abc import ABC, abstractmethod


class InteractionRepository(ABC):
    """
    Abstract class for interaction repository.
    """

    @abstractmethod
    def get_all_interactions(self) -> list[dict]:
        pass