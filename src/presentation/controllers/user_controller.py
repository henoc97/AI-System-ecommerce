import sys
import os

# Ajoutez le chemin du répertoire parent au sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from application.use_cases.user_use_cases.load_user_to_datalake import LoadUserToDatalake
from flask import Blueprint, jsonify

user_controller = Blueprint('user_controller', __name__)

@user_controller.route('/users-hello', methods=['GET'])
def get_users():
    return jsonify({'message': 'Hello, World!'})

@user_controller.route('/load-users-to-datalake', methods=['GET'])
def load_users_to_datalake():
    try:
        load_user_to_datalake = LoadUserToDatalake()
        load_user_to_datalake.execute()
        return jsonify({'message': 'Users loaded to datalake'})
    except Exception as e:
        return jsonify({'message': f'Error loading users to datalake: {e}'})
    