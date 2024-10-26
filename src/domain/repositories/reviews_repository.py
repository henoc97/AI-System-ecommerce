

from abc import ABC, abstractmethod


class ReviewsRepository(ABC):
    """
    Abstract class for reviews repository.
    """

    @abstractmethod
    def get_all_reviews(self) -> list[dict]:
        pass
