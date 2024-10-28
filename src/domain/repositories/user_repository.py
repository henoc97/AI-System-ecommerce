from abc import ABC, abstractmethod

class UserRepository(ABC):
    """
    Abstract class for user repository.
    """

    @abstractmethod
    def get_all_users(self, last_run_time) -> list[dict]:
        pass

    # @abstractmethod
    # def get_user_by_email(self, email: str) -> dict:
    #     pass