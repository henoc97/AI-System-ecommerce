

from domain.repositories.reviews_repository import ReviewsRepository
from infrastructure.database.make_query import make_query


class ReviewRepositoryImpl(ReviewsRepository):
    def __init__(self):
        self.query_executor = make_query()

    def get_all_reviews(self, last_run_time) -> list[dict]:
        try:
            query = f"""SELECT * FROM "Review" WHERE updatedAt > {last_run_time};"""
            reviews = self.query_executor.execute(query)
            return reviews
        except Exception as e:
            print(f"Error getting all reviews: {e}")
            return []
