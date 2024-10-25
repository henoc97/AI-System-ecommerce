import sys
import os

# Ajoutez le chemin du répertoire parent au sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from src.infrastructure.web.server import app

if __name__ == '__main__':
    app.run()
