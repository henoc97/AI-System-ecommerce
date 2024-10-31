
from application.transform_data_to_datalake_raw.feed_datalake_raw_pipeline import FeedRawPipeline
from application.services.user_services import UserServices
from infrastructure.repositories_impl.user_repository_impl import UserRepositoryImpl


class LoadUserToDatalake:
    def __init__(self):
        user_repository = UserRepositoryImpl()
        self.feed_raw = FeedRawPipeline()
        self.user_services = UserServices(user_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.user_services.get_all_users
            self.feed_raw.run_pipeline(get_data, 'raw/users', last_run_time)
        except Exception as e:
            print(f"Error loading users to datalake: {e}")
        

