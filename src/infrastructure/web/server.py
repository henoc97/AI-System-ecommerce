import os
import sys
from flask import Flask


# Ajoutez le chemin du répertoire parent au sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from presentation.controllers.user_controller import user_controller

app = Flask(__name__)

app.register_blueprint(user_controller)

if __name__ == '__main__':
    app.run(debug=True)