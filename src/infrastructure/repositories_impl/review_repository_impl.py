

from domain.repositories.reviews_repository import ReviewsRepository
from infrastructure.database.make_query import MakeQuery


class ReviewRepositoryImpl(ReviewsRepository):
    def __init__(self):
        self.query_executor = MakeQuery()

    def get_all_reviews(self, last_run_time) -> list[dict]:
        try:
            query = 'SELECT * FROM "Review" WHERE "updatedAt" > %s;'
            params = (last_run_time,)
            reviews = self.query_executor.execute(query, params=params)
            return reviews
        except Exception as e:
            print(f"Error getting all reviews: {e}")
            return []
