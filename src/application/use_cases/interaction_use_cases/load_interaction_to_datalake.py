

from application.services.interaction_servive import InteractionServices
from application.transform_data_to_datalake_raw.transform_pipeline import extr_load_data_to_datalake_pipeline
from infrastructure.repositories_impl.intercation_repository_impl import InteractionRepositoryImpl


class LoadInteractionToDatalake:
    def __init__(self):
        interaction_repository = InteractionRepositoryImpl()
        self.interaction_services = InteractionServices(interaction_repository)
        
    def execute(self, last_run_time):
        try:
            get_data = self.interaction_services.get_all_interactions
            extr_load_data_to_datalake_pipeline(get_data, 'raw/interactions', last_run_time)
        except Exception as e:
            print(f"Error loading interactions to datalake: {e}")
            