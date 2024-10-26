
from application.helper.to_csv import preprocess_data
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from application.services.user_services import UserServices
from infrastructure.repositories_impl.user_repository_impl import UserRepositoryImpl


class LoadUserToDatalake:
    def __init__(self):
        user_repository = UserRepositoryImpl()
        self.user_services = UserServices(user_repository)
        
    def execute(self):
        try:
            users = self.user_services.get_all_users()
            output_file = 'src/infrastructure/data_lake/raw/users/users.csv'
            cols = ['id', 'name', 'email', 'role', 'created_at', 'updated_at']
            preprocess_data(users, cols, output_file)
            upload_file_to_s3(output_file, 'raw/users')
        except Exception as e:
            print(f"Error loading users to datalake: {e}")
        

