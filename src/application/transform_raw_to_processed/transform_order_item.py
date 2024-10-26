import pandas as pd

def transform_order_item(df: pd.DataFrame) -> pd.DataFrame | None:
    # Inital df columns : ['id', 'orderId', 'productId', 'quantity', 'price', 'created_at']
    try:
        # Calculate the total price per item
        df['total_price_per_item'] = df['price'] * df['quantity']
        
        # Calculate the total quantity per order
        total_quantity_per_order = df.groupby('orderId')['quantity'].transform('sum')
        df['total_quantity_per_order'] = total_quantity_per_order
        
        order_counts = df.groupby('orderId')['quantity'].transform('count')
        df['average_quantity_per_order'] = total_quantity_per_order / order_counts

        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['day'] = df['created_at'].dt.day
        df['week'] = df['created_at'].dt.isocalendar().week
        df['month'] = df['created_at'].dt.month
        df['year'] = df['created_at'].dt.year
    
        print("Order item transformed successfully:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error transforming order item: {e}")
        return None