from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS

# Créer une session Spark
spark = SparkSession.builder.appName("HybridRecommender").getOrCreate()

# Charger les données d'interactions
df_interactions = spark.read.csv("data_science/src/application/services/recommandation/user_product_interactions.csv", header=True, inferSchema=True)

# Préparer les données
df_interactions = df_interactions.select("user_id", "product_id", "interaction_value")

# Entraîner le modèle ALS
als = ALS(maxIter=10, regParam=0.1, userCol="user_id", itemCol="product_id", ratingCol="interaction_value", coldStartStrategy="drop")
collab_model = als.fit(df_interactions)
