
from infrastructure.database.connection import get_connection


class MakeQuery:
    def __init__(self):
        self.connection = get_connection()

    def execute(self, query: str):
        if self.connection:
            try:
                with self.connection.cursor() as cursor:  # Utilisation du gestionnaire de contexte
                    cursor.execute(query)
                    
                    # Récupérer les résultats
                    interactions = cursor.fetchall()
                    
                    # Récupérer les noms des colonnes
                    column_names = [desc[0] for desc in cursor.description]
                    
                    # Convertir les résultats en liste de dictionnaires
                    results = [dict(zip(column_names, row)) for row in interactions]
                    return results
            except Exception as e:
                print(f"Erreur lors de l'exécution de la requête : {e}")
                return None
        else:
            print("Erreur de connexion à la base de données")
            return None
