import boto3
import pandas as pd
import numpy as np
from io import StringIO
from sklearn.feature_extraction.text import TfidfVectorizer
from datetime import datetime

def transform_product(df: pd.DataFrame) -> pd.DataFrame:
    # Inital df columns : ['id', 'name', 'description', 'price', 'categoryId', 'stock', 'shopId', 'created_at', 'updated_at', 'vendorId']
    try:
        # Remove special characters from the name column
        df['name'] = df['name'].str.lower().str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
        
        # NLP preprocessing for the description column
        # Fill missing values in the description column with an empty string
        df['description'] = df['description'].fillna('')
        # Vectorize the description column
        vectorizer = TfidfVectorizer(stop_words=['english', 'french'])
        tfidf_matrix = vectorizer.fit_transform(df['description'])
        df_tfidf = pd.DataFrame(tfidf_matrix.toarray(), columns=vectorizer.get_feature_names_out())
        df = pd.concat([df, df_tfidf], axis=1)

        # Create price categories based on the price with tax
        df['price_with_tax'] = df['price'] * 1.2
        df['price_category'] = pd.cut(df['price_with_tax'], bins=[0, 50, 200, np.inf], labels=['low', 'medium', 'high'])
        
        # Create a boolean column to indicate if the product is in stock
        df['in_stock'] = df['stock'] > 0
        
        # Calculate the product age
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['product_age'] = (datetime.now() - df['created_at']).dt.days

        # Calculate the updated frequency
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
        df['updated_frequency'] = (datetime.now() - df['updated_at']).dt.days / (df['product_age'] + 1)
        
        print("Product dataframe after successful transformation:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error transforming the product dataframe: {e}")
        return None
