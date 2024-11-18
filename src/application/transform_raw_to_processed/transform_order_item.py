from pyspark.sql.functions import col, sum as spark_sum, count, avg, year, month, dayofmonth, weekofyear
from pyspark.sql import functions as F

from domain.repositories.enums.shipping_method import ShippingMethod

def transform_order_item(df):
    # Inital df columns : ['id', 'orderId', 'productId', 'quantity', 'price', 'createdAt']
    try:
        # Vérifiez si le DataFrame est vide avant de continuer
        if df.isEmpty():
            print("Le DataFrame est vide.")
            return None

        # Calculate the total price per item
        df = df.withColumn("total_price_per_item", col("price") * col("quantity"))
        
        # Définir df_order_counts avant de l'utiliser
        df_order_counts = df.groupBy("orderId").agg(count("orderId").alias("order_count"), spark_sum("quantity").alias("total_quantity_per_order"))
        
        df = df.groupBy("orderId").agg(spark_sum("quantity").alias("order_acount"))
        df = df.join(df_order_counts, on="orderId", how="left")
        df = df.withColumn("average_quantity_per_order", col("total_quantity_per_order") / col("order_count"))
        
        # Vérifiez si la colonne 'createdAt' existe avant de l'utiliser
        if 'createdAt' in df.columns:
            df = df.withColumn("created_at", F.to_timestamp("createdAt", "yyyy-MM-dd'T'HH:mm:ss"))
        else:
            print("La colonne 'createdAt' n'existe pas dans le DataFrame.")
            return None
        
        df = df.withColumn("day", dayofmonth("created_at"))
        df = df.withColumn("month", month("created_at"))
        df = df.withColumn("month", month("created_at"))
        df = df.withColumn("year", year("created_at"))
        
        # Correction de l'encodage de la méthode de livraison
        shipping_method_indexer = F.when(F.col("shippingMethod") == ShippingMethod.STANDARD.value, 0) \
                                   .when(F.col("shippingMethod") == ShippingMethod.EXPRESS.value, 1) \
                                   .otherwise(2)
        df = df.withColumn("shipping_method_encoded", shipping_method_indexer)
    
        print("Order item transformed successfully:")
        print(df.show(5))
        return df
    except Exception as e:
        print(f"Error transforming order item: {e}")
        return None