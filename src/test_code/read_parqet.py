import pandas as pd

# Remplacez le chemin par le chemin vers votre fichier Parquet
file_path = "src/test_code/part-00000-725d49e0-893e-4d80-948a-2912baaeac6e-c000.snappy.parquet"

# Lire le fichier Parquet
try:
    data = pd.read_parquet(file_path)
    # Afficher les premières lignes du fichier pour en avoir un aperçu
    print(data.head())
except Exception as e:
    print("Une erreur est survenue lors de la lecture du fichier :", e)
