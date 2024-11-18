from gen_recs import content_recs, collab_recs

# Dictionnaire final pour les recommandations combinées
combined_recs = {}

def combine_recs():
    # Afficher les types et contenus initiaux pour le debugging
    print("Type de collab_recs:", type(collab_recs))
    print("Contenu de collab_recs:", collab_recs.collect())  # Pour Spark, collecte les données
    print("Type de content_recs:", type(content_recs))
    print("Contenu de content_recs:", content_recs)

    # Parcourir les recommandations collaboratives
    for rec in collab_recs.collect():  # Si collab_recs est un RDD Spark, utilisez collect()
        print("Contenu de rec:", rec)  # Affiche chaque 'rec' pour inspecter sa structure

        # Vérifier si 'rec' contient des recommandations sous le champ 'recommendations'
        if hasattr(rec, 'recommendations') and isinstance(rec.recommendations, list):
            for item in rec.recommendations:
                print("Contenu de item:", item)  # Affiche chaque 'item' pour inspecter sa structure

                # Vérifier les différents types possibles pour 'item'
                if isinstance(item, dict):
                    product_id = item.get('product_id')
                    score = item.get('rating')
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    product_id = item[0]
                    score = item[1]
                elif hasattr(item, 'product_id') and hasattr(item, 'rating'):
                    product_id = item.product_id
                    score = item.rating
                else:
                    print("Format inconnu dans item:", item)
                    continue

                # Ajouter le score au dictionnaire final
                if product_id and isinstance(score, (int, float)):
                    combined_recs[product_id] = combined_recs.get(product_id, 0) + score * 0.7
    
    # Parcourir les recommandations basées sur le contenu
    for rec in content_recs:
        if isinstance(rec, dict) and 'product_id' in rec:
            product_id = rec['product_id']
            score = rec.get('score', 1) * 0.3  # Score par défaut de 1 si 'score' n'est pas présent
        elif isinstance(rec, (list, tuple)) and len(rec) >= 2:
            product_id, score = rec[0], rec[1] * 0.3
        else:
            print("Format inconnu dans rec (content_recs):", rec)
            continue

        # Ajouter au dictionnaire final
        if product_id:
            combined_recs[product_id] = combined_recs.get(product_id, 0) + score

# Combiner les recommandations
combine_recs()

# Trier les recommandations finales par score
final_recommendations = sorted(combined_recs.items(), key=lambda x: x[1], reverse=True)

# Afficher les 5 premières recommandations
print("Recommandations finales :", final_recommendations[:5])
