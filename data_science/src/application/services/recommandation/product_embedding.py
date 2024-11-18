from transformers import AutoTokenizer, AutoModel
import torch

# Charger le modèle Hugging Face
model_name = "distilbert-base-uncased"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

def get_embedding(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True)
    outputs = model(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy()

# Générer les embeddings pour chaque description produit
product_catalog = [
    {'id': 1, 'description': 'This is a great product for outdoor activities.'},
    {'id': 2, 'description': 'This is an amazing product for indoor activities.'},
    {'id': 3, 'description': 'Perfect for sports and fitness enthusiasts.'}
]

product_embeddings = {product['id']: get_embedding(product['description']) for product in product_catalog}