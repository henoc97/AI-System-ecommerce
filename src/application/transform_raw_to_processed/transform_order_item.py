
from pyspark.sql.functions import col, sum as spark_sum, count, avg, year, month, dayofmonth, weekofyear
from pyspark.sql import functions as F

def transform_order_item(df):
    # Inital df columns : ['id', 'orderId', 'productId', 'quantity', 'price', 'created_at']
    try:
        # Calculate the total price per item
        df = df.withColumn("total_price_per_item", col("price") * col("quantity"))
        
        # Calculate the total quantity per order
        df = df.groupBy("orderId").agg(spark_sum("quantity").alias("order_acount"))
        df = df.join(col="order_counts", on="orderId", how="left")
        df = df.withColumn("average_quantity_per_order", col("total_quantity_per_order") / col("order_count"))
        
        df = df.withColumn("created_at", F.to_timestamp("createdAt", "yyyy-MM-dd'T'HH:mm:ss"))
        df = df.withColumn("day", dayofmonth("createdAt"))
        df = df.withColumn("month", month("createdAt"))
        df = df.withColumn("month", month("createdAt"))
        df = df.withColumn("year", year("createdAt"))
    
        print("Order item transformed successfully:")
        df.show(5)
        return df
    except Exception as e:
        print(f"Error transforming order item: {e}")
        return None