

from application.helper.to_csv import preprocess_data
from application.services.interaction_servive import InteractionServices
from infrastructure.data_lake.setup.automate_ingestion import upload_file_to_s3
from infrastructure.repositories_impl.intercation_repository_impl import InteractionRepositoryImpl


class LoadInteractionToDatalake:
    def __init__(self):
        interaction_repository = InteractionRepositoryImpl()
        self.interaction_services = InteractionServices(interaction_repository)
        
    def execute(self):
        try:
            interactions = self.interaction_services.get_all_interactions()
            output_file = 'src/infrastructure/data_lake/raw/interactions/interactions.csv'
            cols = ['id', 'userId', 'action', 'productId', 'created_at']
            preprocess_data(interactions, cols, output_file)
            upload_file_to_s3(output_file, 'raw/interactions')
        except Exception as e:
            print(f"Error loading interactions to datalake: {e}")
            