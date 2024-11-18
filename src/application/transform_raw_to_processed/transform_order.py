from pyspark.sql.functions import col, avg, rank, count, year, month, dayofmonth, weekofyear
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from domain.repositories.enums.order_status import OrderStatus
from domain.repositories.enums.shipping_method import ShippingMethod

def transform_order(df):
    # Inital df columns : ['id', 'userId', 'shopId', 'status', 'totalAmount', 'paymentId', 'trackingNumber', 'shippingMethod', 'createdAt', 'updatedAt']
    try:
                
        # Transform the status to the enum
        df = df.withColumn("is_completed", col("status") == OrderStatus.DELIVERED.value)
        
        # Calcul de la moyenne du montant total de commande
        avg_order_amount = df.agg(avg("totalAmount").alias("average_order_amount")).collect()[0]["average_order_amount"]
        df = df.withColumn("average_order_amount", F.lit(avg_order_amount))

        # Calcul du percentile du montant de la commande
        window_spec = Window.orderBy("totalAmount").partitionBy("userId")
        df = df.withColumn("order_amount_percentile", rank().over(window_spec) / df.count())

        # Calcul du nombre de commandes par utilisateur et de la dépense moyenne par utilisateur
        order_count_by_user = df.groupBy("userId").agg(count("id").alias("order_count_by_user"),
                                                       avg("totalAmount").alias("average_spend_by_user"))
        df = df.join(order_count_by_user, on="userId", how="left")

        # Calcul du temps de traitement
        df = df.withColumn("created_at", F.to_timestamp("createdAt"))
        df = df.withColumn("updated_at", F.to_timestamp("updatedAt"))
        df = df.withColumn("processing_time", (F.col("updatedAt").cast("long") - F.col("createdAt").cast("long")) / (24 * 3600))

        # Encodage de la méthode de livraison
        shipping_method_indexer = F.when(F.col("shippingMethod") == ShippingMethod.STANDARD.value, 0) \
                                   .when(F.col("shippingMethod") == ShippingMethod.EXPRESS.value, 1) \
                                   .otherwise(2)
        df = df.withColumn("shipping_method_encoded", shipping_method_indexer)

        # Extraction des informations de date
        df = df.withColumn("day", dayofmonth("created_at")) \
               .withColumn("week", weekofyear("created_at")) \
               .withColumn("month", month("created_at")) \
               .withColumn("year", year("created_at"))

        # Affichage du DataFrame transformé
        print("Order transformed successfully:")
        print(df.show(5))
        return df
    
    except Exception as e:
        print(f"Error transforming order item: {e}")
        return None
