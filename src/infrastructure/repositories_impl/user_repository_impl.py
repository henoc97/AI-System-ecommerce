from domain.repositories.user_repository import UserRepository
from infrastructure.database.make_query import make_query

class UserRepositoryImpl(UserRepository):
    """
    Implementation of the UserRepository interface.
    """
    def __init__(self) -> None:
        self.query_executor = make_query()
    
    def get_all_users(self) -> list[dict]:
        try:
            query = """SELECT * FROM "User";"""
            result = self.query_executor.execute(query)
            return result
        except:
            print("Erreur lors de la récupération des utilisateurs")
            return []
    

