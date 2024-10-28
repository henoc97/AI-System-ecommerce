from pyspark.sql.functions import col, when, lower, regexp_replace, datediff, current_date, lit
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd

def transform_product(df):
    try:
        # 1. Suppression des caractères spéciaux dans la colonne "name"
        df = df.withColumn("name", lower(regexp_replace(col("name"), r'[^a-zA-Z0-9\s]', '')))
        
        # 2. Prétraitement NLP pour la colonne "description" en utilisant TF-IDF
        # Extraction de la colonne "description" pour transformation en Pandas
        descriptions = df.select("id", "description").fillna("").toPandas()
        
        # Calcul du TF-IDF
        vectorizer = TfidfVectorizer(stop_words=['english', 'french'])
        tfidf_matrix = vectorizer.fit_transform(descriptions['description'])
        df_tfidf = pd.DataFrame(tfidf_matrix.toarray(), columns=vectorizer.get_feature_names_out())
        df_tfidf['id'] = descriptions['id']
        
        # Conversion du DataFrame TF-IDF en DataFrame Spark et jointure sur "id"
        df_tfidf_spark = df.sql_ctx.createDataFrame(df_tfidf)  # Utilisation de `sql_ctx` pour créer directement depuis le contexte de `df`
        df = df.join(df_tfidf_spark, on="id", how="left")

        # 3. Création des catégories de prix
        df = df.withColumn("price_with_tax", col("price") * 1.2)
        df = df.withColumn("price_category", 
                           when(col("price_with_tax") <= 50, "low")
                           .when((col("price_with_tax") > 50) & (col("price_with_tax") <= 200), "medium")
                           .otherwise("high"))

        # 4. Indicateur de stock
        df = df.withColumn("in_stock", col("stock") > 0)

        # 5. Calcul de l'âge du produit
        df = df.withColumn("created_at", col("created_at").cast("timestamp"))
        df = df.withColumn("product_age", datediff(current_date(), col("created_at")))

        # 6. Calcul de la fréquence de mise à jour
        df = df.withColumn("updated_at", col("updated_at").cast("timestamp"))
        df = df.withColumn("updated_frequency", datediff(current_date(), col("updated_at")) / (col("product_age") + lit(1)))

        print("Product dataframe after successful transformation:")
        df.show(5)
        return df

    except Exception as e:
        print(f"Error transforming the product dataframe: {e}")
        return None
