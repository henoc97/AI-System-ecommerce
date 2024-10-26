
from infrastructure.database.connection import get_connection


class make_query:
    def __init__(self):
        self.connection = get_connection()

    def execute(self, query: str):
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        else:
            print("Erreur de connexion à la base de données")
            return None