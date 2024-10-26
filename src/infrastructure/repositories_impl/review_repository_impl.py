

from domain.repositories.reviews_repository import ReviewsRepository
from infrastructure.database.make_query import make_query


class ReviewRepositoryImpl(ReviewsRepository):
    def __init__(self):
        self.query_executor = make_query()

    def get_all_reviews(self) -> list[dict]:
        try:
            query = """SELECT * FROM "Review";"""
            reviews = self.query_executor.execute(query)
            return reviews
        except Exception as e:
            print(f"Error getting all reviews: {e}")
            return []
