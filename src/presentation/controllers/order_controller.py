
from flask import Blueprint, jsonify
from application.use_cases.user_use_cas.load_order_to_datalake import LoadOrderToDatalake


order_controller = Blueprint('order_controller', __name__)

@order_controller.route('/load-orders-to-datalake', methods=['GET'])
def load_orders_to_datalake():
    try:
        load_order_to_datalake = LoadOrderToDatalake()
        load_order_to_datalake.execute()
        return jsonify({'message': 'Orders loaded to datalake'})
    except Exception as e:
        return jsonify({'message': f'Error loading orders to datalake: {e}'})
