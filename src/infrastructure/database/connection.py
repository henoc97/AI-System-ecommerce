import os
import psycopg2
from psycopg2 import sql, OperationalError
from dotenv import load_dotenv


# Charger les variables d'environnement
load_dotenv()

db_host = os.getenv('POSTGRES_HOST')
db_user = os.getenv('POSTGRES_USER')
db_name = os.getenv('DATABASE_NAME')
db_pwd = os.getenv('POSTGRES_PWD')

class DatabaseConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
        db_config = {
            'host': db_host,
            'database': db_name,
            'user': db_user,
            'password': db_pwd
        }
        try:
            self.connection = psycopg2.connect(**db_config)
            self.connection.autocommit = True 
            print("Connexion à la data base réussie.")
        except OperationalError as e:
            print(f"Erreur de connexion : {e}")
            self.connection = None

    def get_connection(self):
        if self.connection is None or self.connection.closed:
            self._initialize_connection()
        return self.connection

    def close_connection(self):
        """
        Ferme la connexion à la base de données.
        """
        if self.connection and not self.connection.closed:
            self.connection.close()
            print("Connexion à la data base fermée.")

# Fonction globale pour récupérer la connexion à la data base
def get_connection():
    return DatabaseConnection().get_connection()
