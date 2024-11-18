from domain.repositories.interaction_repository import InteractionRepository
from infrastructure.database.make_query import MakeQuery


class InteractionRepositoryImpl(InteractionRepository):
    def __init__(self):
        self.query_executor = MakeQuery()

    def get_all_interactions(self, last_run_time) -> list[dict]:
        try:
            query = 'SELECT * FROM "UserActivity" WHERE "updatedAt" > %s;'
            params = (last_run_time,)
            interactions = self.query_executor.execute(query, params=params)
            return interactions
        except Exception as e:
            print(f"Error getting all interactions: {e}")
            return []
