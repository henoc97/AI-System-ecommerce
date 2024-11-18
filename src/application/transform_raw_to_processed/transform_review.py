from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, regexp_replace, count, window, dayofmonth, weekofyear, month, year, lit
from transformers import pipeline
from pyspark.sql import Window

# Initialiser Spark
spark = SparkSession.builder.appName("ReviewTransformation").getOrCreate()

# Chargement du modèle de sentiment Hugging Face
sentiment_analyzer = pipeline('sentiment-analysis', model="nlptown/bert-base-multilingual-uncased-sentiment")

def transform_review(df):
    try:
        # Calcul de la note moyenne par produit
        avg_rating = df.groupBy("productId").agg({"rating": "mean"}).withColumnRenamed("avg(rating)", "average_rating")
        df = df.join(avg_rating, on="productId", how="left")

        # Nettoyage des commentaires
        df = df.withColumn("comment", when(col("comment").isNull(), "").otherwise(col("comment")))
        df = df.withColumn("comment_cleaned", lower(regexp_replace(col("comment"), r'[^a-z\s]', '')))

        # Extraction des commentaires pour la transformation en DataFrame Pandas
        comments_df = df.select("id", "comment_cleaned").toPandas()

        # Analyse de sentiment en bloc
        comments_df['sentiment'] = comments_df['comment_cleaned'].apply(lambda x: sentiment_analyzer(x)[0]['label'])
        comments_df['sentiment_score'] = comments_df['comment_cleaned'].apply(lambda x: sentiment_analyzer(x)[0]['score'])

        # Conversion du DataFrame Pandas avec sentiments en DataFrame Spark et jointure
        sentiments_spark_df = spark.createDataFrame(comments_df[['id', 'sentiment', 'sentiment_score']])
        df = df.join(sentiments_spark_df, on="id", how="left")

        # Nombre de critiques par utilisateur
        user_window = Window.partitionBy("userId")
        df = df.withColumn("reviews_by_user", count("id").over(user_window))

        # Extraction du jour, de la semaine et du mois de la création de la critique
        df = df.withColumn("created_at", col("createdAt").cast("timestamp"))
        df = df.withColumn("day", dayofmonth("created_at"))
        df = df.withColumn("week", weekofyear("created_at"))
        df = df.withColumn("month", month("created_at"))
        df = df.withColumn("year", year("created_at"))

        # Indicateur des critiques de haute qualité
        df = df.withColumn("is_high_quality", (col("verified") == True) & (col("flagged") == False))

        print("Review transformed successfully:")
        print(df.show(5))
        return df

    except Exception as e:
        print(f"Error transforming review: {e}")
        return None

# Exemple d'utilisation
# df_spark = spark.read.csv("path_to_your_file.csv", header=True, inferSchema=True)
# transform_review(df_spark)
