
from application.helper.to_csv import preprocess_data
from application.transform_data_to_datalake_raw.transform_pipeline import extr_load_data_to_datalake_pipeline
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from application.services.user_services import UserServices
from infrastructure.repositories_impl.user_repository_impl import UserRepositoryImpl


class LoadUserToDatalake:
    def __init__(self):
        user_repository = UserRepositoryImpl()
        self.user_services = UserServices(user_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.user_services.get_all_users
            extr_load_data_to_datalake_pipeline(get_data, 'raw/users', last_run_time)
        except Exception as e:
            print(f"Error loading users to datalake: {e}")
        

