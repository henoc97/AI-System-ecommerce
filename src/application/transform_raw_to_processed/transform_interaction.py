
from pyspark.sql.functions import col, when, lit, year, month, dayofmonth, hour
from pyspark.sql import functions as F
from domain.repositories.enums.user_interaction import UserActivityAction

def transform_interaction(df):
    # Inital df columns : ['id', 'userId', 'action', 'productId', 'createdAt']
    try:
        # Encode the action
        action_mapping = {
            UserActivityAction.VIEW_PRODUCT: 1,
            UserActivityAction.ADD_TO_CART: 2,
            UserActivityAction.PURCHASE: 3,
            UserActivityAction.OTHER: 0
        }
        
        # UserActivity Action
        df = df.withColumn(
            "view_product_count",
            when(col("action") == UserActivityAction.VIEW_PRODUCT, 1).otherwise(0)
        ).withColumn(
            "add_to_cart_count",
            when(col("action") == UserActivityAction.ADD_TO_CART, 1).otherwise(0)
        ).withColumn(
            "purchase_count",
            when(col("action") == UserActivityAction.PURCHASE, 1).otherwise(0)
        )
        
        df = df.groupby("userId").agg(
            F.sum("view_product_count").alias("view_product_count"),
            F.sum("add_to_cart_count").alias("add_to_cart_count"),
            F.sum("purchase_count").alias("purchase_count")
        )
        
        df = df.withColumn("action_encoded", when(col("action") == UserActivityAction.VIEW_PRODUCT, lit(1))
                                            .when(col("action") == UserActivityAction.ADD_TO_CART, lit(2))
                                            .when(col("action") == UserActivityAction.PURCHASE, lit(3))
                                            .otherwise(0))
        
        df = df.withColumn("created_at", F.to_timestamp("createdAt", "yyyy-MM-dd'T'HH:mm:ss"))
        df = df.withColumn("hour", hour("createdAt"))
        df = df.withColumn("day", dayofmonth("createdAt"))
        df = df.withColumn("month", month("createdAt"))
        df = df.withColumn("month", month("createdAt"))
        df = df.withColumn("year", year("createdAt"))
        
        print("Interaction transformed successfully:")
        df.show(5)
        return df
    
    except Exception as e:
        print(f"Error transforming interaction: {e}")
        return None
