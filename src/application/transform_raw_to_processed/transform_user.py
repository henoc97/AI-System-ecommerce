from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, trim, hash, lit, current_date, datediff
from pyspark.sql.types import IntegerType
from domain.repositories.enums.user_role import UserRole


def transform_user(df):
    try:
        # 1. Nettoyage des noms : suppression des espaces et mise en majuscule des premières lettres
        df = df.withColumn("name", trim(regexp_replace(col("name"), r'^\s+|\s+$', '')))
        df = df.withColumn("name", col("name").alias("name"))

        # Suppression des doublons en utilisant Spark
        df = df.dropDuplicates(["id", "name"])

        # 2. Anonymisation de l'email en utilisant un hash
        df = df.withColumn("email", hash(col("email")))

        # 3. Encodage des rôles d'utilisateur
        role_mapping = {UserRole.CLIENT: 1, UserRole.ADMIN: 2, UserRole.SELLER: 3}
        role_mapping_expr = when(col("role") == UserRole.CLIENT, lit(1)) \
            .when(col("role") == UserRole.ADMIN, lit(2)) \
            .when(col("role") == UserRole.SELLER, lit(3)) \
            .otherwise(lit(None))
        df = df.withColumn("role_encoded", role_mapping_expr)

        # 4. Calcul de l'ancienneté du client en jours et segmentation
        df = df.withColumn("created_at", col("created_at").cast("timestamp"))
        df = df.withColumn("customer_age", datediff(current_date(), col("created_at")))
        df = df.withColumn("segment",
                           when(col("customer_age") <= 180, lit("new"))
                           .when((col("customer_age") > 180) & (col("customer_age") <= 365), lit("regular"))
                           .otherwise(lit("loyal")))

        # 5. Suivi des mises à jour
        df = df.withColumn("updated_at", col("updated_at").cast("timestamp"))
        df = df.withColumn("days_since_update", datediff(current_date(), col("updated_at")))
        df = df.withColumn("is_active", col("days_since_update") < 30)  # Actif si mis à jour dans les 30 derniers jours

        print("User transformed successfully:")
        df.show(5)
        return df

    except Exception as e:
        print(f"Error transforming user: {e}")
        return None
