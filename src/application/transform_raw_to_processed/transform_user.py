

from datetime import datetime
import pandas as pd

from domain.repositories.enums.user_role import UserRole


def transform_user(df: pd.DataFrame) -> pd.DataFrame | None:
    try:
        # Cleaning the name
        df['name'] = df['name'].str.strip().str.title()
        df = df.drop_duplicates(subset=['id', 'name'])  # Drop duplicates based on id and name
        
        # Anonymize the email
        df['email'] = df['email'].apply(lambda x: hash(x))  # Replace emails by hash for anonymization
        
        # Encode the role
        role_mapping = {UserRole.CLIENT: 1, UserRole.ADMIN: 2, UserRole.SELLER: 3}
        df['role_encoded'] = df['role'].map(role_mapping)
        
        # Calculate the customer age and segment
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['customer_age'] = (datetime.now() - df['created_at']).dt.days
        df['segment'] = pd.cut(df['customer_age'], bins=[0, 180, 365, float('inf')], labels=['new', 'regular', 'loyal'])
        
        # Track the updates
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        df['days_since_update'] = (datetime.now() - df['updated_at']).dt.days
        df['is_active'] = df['days_since_update'] < 30  # Mark as active if updated in the last 30 days
        
        print("User transformed successfully:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error transforming user: {e}")
        return None
