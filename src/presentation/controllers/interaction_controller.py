

from flask import Blueprint, jsonify

from application.use_cases.user_use_cas.load_interaction_to_datalake import LoadInteractionToDatalake


interaction_controller = Blueprint('interaction_controller', __name__)

@interaction_controller.route('/load-interactions-to-datalake', methods=['GET'])
def load_interactions_to_datalake():
    try:
        load_interactions = LoadInteractionToDatalake()
        load_interactions.execute()
        return jsonify({'message': 'interactions loaded to datalake'})
    except Exception as e:
        return jsonify({'message': f'Error loading interactions to datalake: {e}'})
