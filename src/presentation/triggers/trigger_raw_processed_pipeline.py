import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.use_cases.interaction_use_cases.load_interaction_to_processed import LoadInteractionToProcessed
from application.use_cases.order_item_use_cases.load_order_item_to_processed import LoadOrderItemToProcessed
from application.use_cases.order_use_cases.load_order_to_processed import LoadOrderToProcessed
from application.use_cases.product_use_cases.load_product_to_processed import LoadProductToProcessed
from application.use_cases.review_use_cases.load_review_to_processed import LoadReviewToProcessed
from application.use_cases.user_use_cases.load_user_to_processed import LoadUserToProcessed


def transform_raw_to_processed_pipeline():
    try:
        load_interaction_to_processed = LoadInteractionToProcessed()
        load_order_item_to_processed = LoadOrderItemToProcessed()
        load_order_to_processed = LoadOrderToProcessed()
        load_product_to_processed = LoadProductToProcessed()
        load_review_to_processed = LoadReviewToProcessed()
        load_user_to_processed = LoadUserToProcessed()
        
        load_interaction_to_processed.execute()
        load_order_item_to_processed.execute()
        load_order_to_processed.execute()
        load_review_to_processed.execute()
        load_product_to_processed.execute()
        load_user_to_processed.execute()
        print("All raws data have been successful transformation to processeds data.")
    except Exception as e:
        print(f"Error trigging transformation raw to processed data : {e}")
        

if __name__ == '__main__':
    transform_raw_to_processed_pipeline()