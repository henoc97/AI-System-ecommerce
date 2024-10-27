
from flask import Blueprint, jsonify
from application.use_cases.product_use_cases.load_product_to_datalake import LoadProductToDatalake


product_controller = Blueprint('product_controller', __name__)

@product_controller.route('/load-products-to-datalake', methods=['GET'])
def load_products_to_datalake():
    try:
        load_product_to_datalake = LoadProductToDatalake()
        load_product_to_datalake.execute()
        return jsonify({'message': 'Products loaded to datalake'})  
    except Exception as e:
        return jsonify({'message': f'Error loading products to datalake: {e}'})
