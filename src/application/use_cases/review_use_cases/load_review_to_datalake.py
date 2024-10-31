
from application.services.review_serrvice import ReviewServices
from application.transform_data_to_datalake_raw.feed_datalake_raw_pipeline import FeedRawPipeline
from infrastructure.repositories_impl.review_repository_impl import ReviewRepositoryImpl


class LoadReviewToDatalake:
    def __init__(self):
        review_repository = ReviewRepositoryImpl()
        self.feed_raw = FeedRawPipeline()
        self.review_services = ReviewServices(review_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.review_services.get_all_reviews
            self.feed_raw.run_pipeline(get_data, 'raw/reviews', last_run_time)
        except Exception as e:
            print(f"Error loading reviews to datalake: {e}")
            