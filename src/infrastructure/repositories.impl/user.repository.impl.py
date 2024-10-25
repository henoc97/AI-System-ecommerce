from infrastructure.database.connection import get_connection
from domain.repositories.user_repository import UserRepository
class UserRepositoryImpl(UserRepository):
    """
    Implementation of the UserRepository interface.
    """
    def __init__(self):
        self.connection = get_connection()

    def get_all_users(self) -> list[dict]:
        query = "SELECT * FROM users;"
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            users = cursor.fetchall()
            return users
        else:
            return []
