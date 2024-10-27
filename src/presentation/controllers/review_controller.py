from flask import Blueprint, jsonify
from application.use_cases.review_use_cases.load_review_to_datalake import LoadReviewToDatalake


review_controller = Blueprint('review_controller', __name__)

@review_controller.route('/load-reviews-to-datalake', methods=['GET'])
def load_reviews_to_datalake():
    try:
        load_reviews_to_datalake = LoadReviewToDatalake()
        load_reviews_to_datalake.execute()
        return jsonify({'message': 'reviews loaded to datalake'})  
    except Exception as e:
        return jsonify({'message': f'Error loading reviews to datalake: {e}'})
