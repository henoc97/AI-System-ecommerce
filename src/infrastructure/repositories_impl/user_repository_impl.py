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
            query = f'SELECT * FROM "User" WHERE "updatedAt" > \'{last_run_time}\';'
            result = self.query_executor.execute(query)
            return result
        except:
            print("Erreur lors de la récupération des utilisateurs")
            return []
    

