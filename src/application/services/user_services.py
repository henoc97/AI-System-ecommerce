from domain.repositories.user_repository import UserRepository

class UserServices:
    def __init__(self, user_repository: UserRepository):
        self.user_repository = user_repository
        
    def get_all_users(self):
        return self.user_repository.get_all_users()

