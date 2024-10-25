import csv

def preprocess_data(data, cols, output_file):
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(cols)  # En-têtes des colonnes
        for line in data:
            writer.writerow(line)  # Écrit chaque ligne dans le fichier
