

from domain.repositories.interaction_repository import InteractionRepository
from infrastructure.database.make_query import make_query


class InteractionRepositoryImpl(InteractionRepository):
    def __init__(self):
        self.query_executor = make_query()

    def get_all_interactions(self) -> list[dict]:
        try:
            query = """SELECT * FROM "UserActivity";"""
            interactions = self.query_executor.execute(query)
            return interactions
        except Exception as e:
            print(f"Error getting all interactions: {e}")
            return []
