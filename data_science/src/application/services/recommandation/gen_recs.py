import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from collab_filter import collab_model, df_interactions
from product_embedding import get_embedding
from product_embedding import product_embeddings


# Recommandations collaboratives pour l'utilisateur
user_id = 2
collab_recs = collab_model.recommendForUserSubset(df_interactions.filter(df_interactions.user_id == user_id), numItems=5)
collab_recs.show()

# Créer un embedding pour le profil utilisateur basé sur les produits consultés
user_history = ["product_description_1", "product_description_2"]  # Description des produits de l'historique
user_embedding = np.mean([get_embedding(desc) for desc in user_history], axis=0)

# Comparer l'embedding utilisateur aux embeddings du catalogue produit
content_recs = sorted(product_embeddings.keys(), key=lambda pid: cosine_similarity(user_embedding, product_embeddings[pid])[0], reverse=True)[:5]
