import sys
sys.path.append('/mnt/c/Users/amavi/projects-studio/ecommerce/AI-System/src')

from application.helper.etl_orchestrator import ETLOrchestrator
from application.use_cases.interaction_use_cases.load_interaction_to_datalake import LoadInteractionToDatalake
from application.use_cases.order_item_use_cases.load_order_item_to_datalake import LoadOrderItemToDatalake
from application.use_cases.order_use_cases.load_order_to_datalake import LoadOrderToDatalake
from application.use_cases.product_use_cases.load_product_to_datalake import LoadProductToDatalake
from application.use_cases.review_use_cases.load_review_to_datalake import LoadReviewToDatalake
from application.use_cases.user_use_cases.load_user_to_datalake import LoadUserToDatalake

loaders = [
    {"loader": LoadInteractionToDatalake(), "root": "raw", "root_child_name": "interactions"},
    {"loader": LoadOrderItemToDatalake(), "root": "raw", "root_child_name": "order_items"},
    {"loader": LoadOrderToDatalake(), "root": "raw", "root_child_name": "orders"},
    {"loader": LoadProductToDatalake(), "root": "raw", "root_child_name": "products"},
    {"loader": LoadReviewToDatalake(), "root": "raw", "root_child_name": "reviews"},
    {"loader": LoadUserToDatalake(), "root": "raw", "root_child_name": "users"}
]

if __name__ == '__main__':
    etl_orchestor = ETLOrchestrator(loaders)
    etl_orchestor.execute_all()