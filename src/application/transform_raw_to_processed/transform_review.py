import pandas as pd
from transformers import pipeline

# NPL hugging face pipeline : sentiment-analysis
sentiment_analyzer = pipeline('sentiment-analysis', model="nlptown/bert-base-multilingual-uncased-sentiment")


def transform_review(df: pd.DataFrame) -> pd.DataFrame | None:
    try:
        # Calculate the average rating per product
        df['average_rating'] = df.groupby('productId')['rating'].transform('mean')

        # Clean the comment
        df['comment'] = df['comment'].fillna('')
        df['comment_cleaned'] = df['comment'].str.lower().str.replace(r'[^a-z\s]', '', regex=True)

        # Get the sentiment of the comment
        df['sentiment'] = df['comment_cleaned'].apply(lambda x: sentiment_analyzer(x)[0]['label'])
        df['sentiment_score'] = df['comment_cleaned'].apply(lambda x: sentiment_analyzer(x)[0]['score'])
        
        # Calculate the number of reviews per user
        df['reviews_by_user'] = df.groupby('userId')['id'].transform('count')

        # Extract the day, week and month of the review
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['day'] = df['created_at'].dt.day
        df['week'] = df['created_at'].dt.isocalendar().week
        df['month'] = df['created_at'].dt.month

        # Flag the high quality reviews
        df['is_high_quality'] = (df['verified'] == True) & (df['flagged'] == False)
        
        print("Review transformed successfully:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error transforming review: {e}")
        return None
