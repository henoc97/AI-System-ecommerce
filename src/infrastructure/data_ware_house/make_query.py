from infrastructure.data_ware_house.connection import get_connection

class MakeQuery:
    def __init__(self):
        self.connection = get_connection()

    def execute(self, query: str, params=None):
        """
        Exécute une requête SQL et retourne les résultats pour les requêtes SELECT.
        Pour les autres types de requêtes, elle retourne le nombre de lignes affectées.
        
        Args:
            query (str): La requête SQL à exécuter.
            params (tuple, optional): Les paramètres de la requête SQL. Defaults to None.
        
        Returns:
            list[dict] | int | None: Liste de dictionnaires pour SELECT, nombre de lignes affectées
                                      pour les autres, ou None en cas d'erreur.
        """
        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(query, params)

                    # Si la requête est de type SELECT, retourner les résultats
                    if query.strip().upper().startswith("SELECT"):
                        data = cursor.fetchall()
                        column_names = [desc[0] for desc in cursor.description]
                        results = [dict(zip(column_names, row)) for row in data]
                        return results
                    else:
                        # Pour INSERT, UPDATE, DELETE, etc., retourne le nombre de lignes affectées
                        self.connection.commit()
                        return cursor.rowcount
            except Exception as e:
                print(f"Erreur lors de l'exécution de la requête : {e}")
                return None
        else:
            print("Erreur de connexion à la base de données")
            return None
