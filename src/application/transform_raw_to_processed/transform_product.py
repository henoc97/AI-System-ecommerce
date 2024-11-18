from pyspark.sql.functions import col, when, lower, regexp_replace, datediff, current_date, lit, year, month, dayofmonth
from sklearn.feature_extraction.text import TfidfVectorizer
from pyspark.sql import functions as F
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
        df = df.withColumn("created_at", col("createdAt").cast("timestamp"))
        df = df.withColumn("product_age", datediff(current_date(), col("createdAt")))

        # 6. Calcul de la fréquence de mise à jour
        df = df.withColumn("updated_at", col("updatedAt").cast("timestamp"))
        df = df.withColumn("updated_frequency", datediff(current_date(), col("updatedAt")) / (col("product_age") + lit(1)))

        df = df.withColumn("created_at", F.to_timestamp("createdAt", "yyyy-MM-dd'T'HH:mm:ss"))
        df = df.withColumn("day", dayofmonth("created_at"))
        df = df.withColumn("month", month("created_at"))
        df = df.withColumn("month", month("created_at"))
        df = df.withColumn("year", year("created_at"))
        print("Product dataframe after successful transformation:")
        print(df.show(5))
        return df

    except Exception as e:
        print(f"Error transforming the product dataframe: {e}")
        return None
