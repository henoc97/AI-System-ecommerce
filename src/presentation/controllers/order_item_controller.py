

from flask import Blueprint, jsonify

from application.use_cases.user_use_cas.load_order_item_to_datalake import LoadOrderItemToDatalake


order_item_controller = Blueprint('order_item_controller', __name__)


@order_item_controller.route('/load-order-items-to-datalake', methods=['GET'])
def load_order_items_to_datalake():
    try:
        load_order_items = LoadOrderItemToDatalake()
        load_order_items.execute()
        return jsonify({'message': 'Order items loaded to datalake'})
    except Exception as e:
        return jsonify({'message': f'Error loading order items to datalake: {e}'})
