from pyspark.sql.functions import col, when, lit, year, month, dayofmonth, hour
from pyspark.sql import functions as F
from domain.repositories.enums.user_interaction import UserActivityAction

def transform_interaction(df):
    # Inital df columns : ['id', 'userId', 'action', 'productId', 'createdAt']
    try:
        # Encode the action
        action_mapping = {
            UserActivityAction.VIEW_PRODUCT.value: 1,
            UserActivityAction.ADD_TO_CART.value: 2,
            UserActivityAction.PURCHASE.value: 3,
            UserActivityAction.OTHER.value: 0
        }
        
        # UserActivity Action
        df = df.withColumn(
            "view_product_count",
            when(col("action") == UserActivityAction.VIEW_PRODUCT.value, 1).otherwise(0)
        ).withColumn(
            "add_to_cart_count",
            when(col("action") == UserActivityAction.ADD_TO_CART.value, 1).otherwise(0)
        ).withColumn(
            "purchase_count",
            when(col("action") == UserActivityAction.PURCHASE.value, 1).otherwise(0)
        )
        
        # Ajout de la colonne action_encoded avant l'agrégation
        df = df.withColumn("action_encoded", 
            when(col("action") == UserActivityAction.VIEW_PRODUCT.value, lit(1))
            .when(col("action") == UserActivityAction.ADD_TO_CART.value, lit(2))
            .when(col("action") == UserActivityAction.PURCHASE.value, lit(3))
            .otherwise(0)
        )
        
        # Ajout de la colonne createdAt avant l'agrégation
        df = df.withColumn("created_at", F.to_timestamp("createdAt", "yyyy-MM-dd'T'HH:mm:ss"))
        
        df = df.groupby("userId").agg(
            F.sum("view_product_count").alias("view_product_count"),
            F.sum("add_to_cart_count").alias("add_to_cart_count"),
            F.sum("purchase_count").alias("purchase_count"),
            F.first("created_at").alias("created_at")  # Conserver la première valeur de created_at
        )
        
        df = df.withColumn("hour", hour("created_at"))
        df = df.withColumn("day", dayofmonth("created_at"))
        df = df.withColumn("month", month("created_at"))
        df = df.withColumn("month", month("created_at"))
        df = df.withColumn("year", year("created_at"))
        
        print("Interaction transformed successfully:")
        print(df.show(5))
        return df
    
    except Exception as e:
        print(f"Error transforming interaction: {e}")
        return None
