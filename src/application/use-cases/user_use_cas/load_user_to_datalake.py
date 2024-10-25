
from application.helper.to_csv import preprocess_data
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from application.services.user_services import UserServices


class LoadUserToDatalake:
    def __init__(self):
        self.user_services = UserServices()
        
    def execute(self):
        try:
            users = self.user_services.get_all_users()
            output_file = 'users.csv'
            cols = ['id', 'name', 'email', 'role', 'created_at', 'updated_at']
            preprocess_data(users, cols, output_file)
            upload_file_to_s3(output_file, 'raw/users')
        except Exception as e:
            print(f"Error loading users to datalake: {e}")
        

