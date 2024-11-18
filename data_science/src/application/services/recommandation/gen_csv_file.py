import pandas as pd
import numpy as np
import random

# Paramètres de la simulation
num_users = 1000  # Nombre d'utilisateurs
num_products = 500  # Nombre de produits
interaction_types = ["view", "add_to_cart", "purchase"]

# Générer des interactions
data = []
for user_id in range(1, num_users + 1):
    # Chaque utilisateur interagit avec un nombre aléatoire de produits
    products_interacted = random.sample(range(1, num_products + 1), k=random.randint(1, 20))
    
    for product_id in products_interacted:
        interaction_type = random.choice(interaction_types)
        if interaction_type == "view":
            interaction_value = 0.1
        elif interaction_type == "add_to_cart":
            interaction_value = 0.5
        elif interaction_type == "purchase":
            interaction_value = 1.0
        
        data.append([user_id, product_id, interaction_type, interaction_value])

# Créer un DataFrame
df = pd.DataFrame(data, columns=["user_id", "product_id", "interaction_type", "interaction_value"])

# Sauvegarder en CSV
df.to_csv("user_product_interactions.csv", index=False)

print("Fichier 'user_product_interactions.csv' généré avec succès !")
