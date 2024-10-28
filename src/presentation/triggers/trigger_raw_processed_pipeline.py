import sys

sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.helper.etl_orchestrator import ETLOrchestrator
from application.use_cases.interaction_use_cases.load_interaction_to_processed import LoadInteractionToProcessed
from application.use_cases.order_item_use_cases.load_order_item_to_processed import LoadOrderItemToProcessed
from application.use_cases.order_use_cases.load_order_to_processed import LoadOrderToProcessed
from application.use_cases.product_use_cases.load_product_to_processed import LoadProductToProcessed
from application.use_cases.review_use_cases.load_review_to_processed import LoadReviewToProcessed
from application.use_cases.user_use_cases.load_user_to_processed import LoadUserToProcessed


loaders = [
    {"loader": LoadInteractionToProcessed(), "root": "processed", "root_child_name": "interactions"},
    {"loader": LoadOrderItemToProcessed(), "root": "processed", "root_child_name": "order_items"},
    {"loader": LoadOrderToProcessed(), "root": "processed", "root_child_name": "orders"},
    {"loader": LoadProductToProcessed(), "root": "processed", "root_child_name": "products"},
    {"loader": LoadReviewToProcessed(), "root": "processed", "root_child_name": "reviews"},
    {"loader": LoadUserToProcessed(), "root": "processed", "root_child_name": "users"}
]

if __name__ == '__main__':
    etl_orchestor = ETLOrchestrator(loaders)
    etl_orchestor.execute_all()