from domain.repositories.user_repository import UserRepository
from infrastructure.database.make_query import MakeQuery

class UserRepositoryImpl(UserRepository):
    """
    Implementation of the UserRepository interface.
    """
    def __init__(self) -> None:
        self.query_executor = MakeQuery()
    
    def get_all_users(self, last_run_time) -> list[dict]:
        try:
            query = 'SELECT * FROM "User" WHERE "updatedAt" > %s;'
            params = (last_run_time,)
            result = self.query_executor.execute(query, params=params)
            return result
        except:
            print("Erreur lors de la récupération des utilisateurs")
            return []
    

