from domain.repositories.interaction_repository import InteractionRepository

class InteractionServices:
    def __init__(self, interaction_repository: InteractionRepository):
        self.interaction_repository = interaction_repository
        
    def get_all_interactions(self) -> list[dict]:
        return self.interaction_repository.get_all_interactions()