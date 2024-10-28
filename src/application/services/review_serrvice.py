

from domain.repositories.reviews_repository import ReviewsRepository


class ReviewServices:
    def __init__(self, reviews_repository: ReviewsRepository):
        self.reviews_repository = reviews_repository

    def get_all_reviews(self, last_run_time) -> list[dict]:
        return self.reviews_repository.get_all_reviews(last_run_time)
