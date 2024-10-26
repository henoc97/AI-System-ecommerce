import pandas as pd

from domain.repositories.enums.user_interaction import UserActivityAction

def transform_interaction(df: pd.DataFrame) -> pd.DataFrame | None:
    # Inital df columns : ['id', 'userId', 'action', 'productId', 'created_at']
    try:
        # Calculate the number of view product, add to cart and purchase per user
        df['view_product_count'] = df[df['action'] == UserActivityAction.VIEW_PRODUCT].groupby('userId')['action'].transform('count')
        df['add_to_cart_count'] = df[df['action'] == UserActivityAction.ADD_TO_CART].groupby('userId')['action'].transform('count')
        df['purchase_count'] = df[df['action'] == UserActivityAction.PURCHASE].groupby('userId')['action'].transform('count')
        
        # Fill the NaN values with 0
        df[['view_product_count', 'add_to_cart_count', 'purchase_count']] = df[
            ['view_product_count', 'add_to_cart_count', 'purchase_count']
        ].fillna(0)

        # Encode the action
        action_mapping = {
            UserActivityAction.VIEW_PRODUCT: 1,
            UserActivityAction.ADD_TO_CART: 2,
            UserActivityAction.PURCHASE: 3,
            UserActivityAction.OTHER: 0
        }
        df['action_encoded'] = df['action'].map(action_mapping)

        # Extract the hour, day, month and year of the interaction
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['hour'] = df['created_at'].dt.hour
        df['day'] = df['created_at'].dt.day
        df['month'] = df['created_at'].dt.month
        df['year'] = df['created_at'].dt.year
        
        print("Interaction transformed successfully:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error transforming interaction: {e}")
        return None
