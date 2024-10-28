
from application.helper.to_csv import preprocess_data
from application.services.review_serrvice import ReviewServices
from application.transform_data_to_datalake_raw.transform_pipeline import extr_load_data_to_datalake_pipeline
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.repositories_impl.review_repository_impl import ReviewRepositoryImpl


class LoadReviewToDatalake:
    def __init__(self):
        review_repository = ReviewRepositoryImpl()
        self.review_services = ReviewServices(review_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.review_services.get_all_reviews
            extr_load_data_to_datalake_pipeline(get_data, 'raw/reviews', last_run_time)
        except Exception as e:
            print(f"Error loading reviews to datalake: {e}")
            