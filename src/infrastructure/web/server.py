import os
import sys
from flask import Flask


# Ajoutez le chemin du répertoire parent au sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from presentation.controllers.interaction_controller import interaction_controller
from presentation.controllers.order_item_controller import order_item_controller
from presentation.controllers.review_controller import review_controller
from presentation.controllers.order_controller import order_controller
from presentation.controllers.product_controller import product_controller
from presentation.controllers.user_controller import user_controller

app = Flask(__name__)

app.register_blueprint(user_controller)
app.register_blueprint(product_controller)
app.register_blueprint(order_controller)
app.register_blueprint(order_item_controller)
app.register_blueprint(review_controller)
app.register_blueprint(interaction_controller)



if __name__ == '__main__':
    app.run(debug=True)