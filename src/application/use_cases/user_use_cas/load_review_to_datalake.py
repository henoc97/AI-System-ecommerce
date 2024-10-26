
from application.helper.to_csv import preprocess_data
from application.services.review_serrvice import ReviewServices
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.repositories_impl.review_repository_impl import ReviewRepositoryImpl


class LoadReviewToDatalake:
    def __init__(self):
        review_repository = ReviewRepositoryImpl()
        self.review_services = ReviewServices(review_repository)
        
    def execute(self):
        try:
            reviews = self.review_services.get_all_reviews()
            output_file = 'src/infrastructure/data_lake/raw/reviews/reviews.csv'
            cols = ['id', 'productId', 'userId', 'rating', 'comment', 'created_at', 'flagged', 'verified']
            preprocess_data(reviews, cols, output_file)
            upload_file_to_s3(output_file, 'raw/reviews')
        except Exception as e:
            print(f"Error loading reviews to datalake: {e}")
            