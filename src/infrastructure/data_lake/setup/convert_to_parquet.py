import pandas as pd


def convert_to_parquet(file_path):
    """
    Converts a CSV file to a Parquet file.
    """
    df = pd.read_csv(file_path)
    output_path = f"{file_path}.parquet"
    df.to_parquet(output_path, engine='pyarrow')
    return output_path

print(convert_to_parquet("../data-exemples/jewelery.csv"))