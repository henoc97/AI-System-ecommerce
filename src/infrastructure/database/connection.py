import psycopg2

class DatabaseConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
        db_config = {
            'host': '172.31.80.1',
            'database': 'ecommerce',
            'user': 'postgres',
            'password': 'henoc2004'
        }
        try:
            self.connection = psycopg2.connect(**db_config)
            print("Connexion à la base de données réussie.")
        except Exception as e:
            print(f"Erreur de connexion : {e}")
            self.connection = None

    def get_connection(self):
        return self.connection
    
def get_connection():
    return DatabaseConnection().get_connection()