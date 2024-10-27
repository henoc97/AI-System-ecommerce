import pandas as pd

from domain.repositories.enums.order_status import OrderStatus

def transform_order(df: pd.DataFrame) -> pd.DataFrame | None:
    # Inital df columns : ['id', 'userId', 'shopId', 'status', 'totalAmount', 'paymentId', 'trackingNumber', 'shippingMethod', 'createdAt', 'updatedAt']
    try:
        
        # Transform the status to the enum
        df['is_completed'] = df['status'] == OrderStatus.DELIVERED.value
        
        # Calculate the average order amount
        average_order_amount = df['totalAmount'].mean()
        df['average_order_amount'] = average_order_amount
        
        # Calculate the percentile of the order amount
        df['order_amount_percentile'] = df['totalAmount'].rank(pct=True)
        
        # Calculate the number of orders per user and the average spend by user
        order_count_by_user = df.groupby('userId')['id'].transform('count')
        df['order_count_by_user'] = order_count_by_user
        df['average_spend_by_user'] = df.groupby('userId')['totalAmount'].transform('mean')
        
        # Calculate the processing time
        df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')
        df['updatedAt'] = pd.to_datetime(df['updatedAt'], errors='coerce')
        df['processing_time'] = (df['updatedAt'] - df['createdAt']).dt.days
        
        # Calculate the shipping method
        df['shipping_method_encoded'] = df['shippingMethod'].astype('category').cat.codes
        
        # Extract the day, week and month of the order
        df['day'] = df['createdAt'].dt.day
        df['week'] = df['createdAt'].dt.isocalendar().week
        df['month'] = df['createdAt'].dt.month
        df['year'] = df['createdAt'].dt.year
        
        print("Order transformed successfully:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error transforming order item: {e}")
        return None
