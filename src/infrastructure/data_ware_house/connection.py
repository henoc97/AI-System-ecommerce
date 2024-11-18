import os
import psycopg2
from psycopg2 import sql, OperationalError
from dotenv import load_dotenv


# Charger les variables d'environnement
load_dotenv()

dwh_host = os.getenv('POSTGRES_HOST')
dwh_user = os.getenv('POSTGRES_USER')
dwh_name = os.getenv('DATAWAREHOUSE_NAME')
dwh_pwd = os.getenv('POSTGRES_PWD')

class DataWareHouseConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataWareHouseConnection, cls).__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
        dwh_config = {
            'host': dwh_host,
            'database': dwh_name,
            'user': dwh_user,
            'password': dwh_pwd
        }
        try:
            self.connection = psycopg2.connect(**dwh_config)
            self.connection.autocommit = True 
            print("Connexion à la data warehouse réussie.")
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
            print("Connexion à la data warehouse fermée.")

# Fonction globale pour récupérer la connexion à la data warehouse
def get_connection():
    return DataWareHouseConnection().get_connection()
