import os
import pandas as pd


def list_csv_files_in_dir(directory):
    csv_files = []
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            csv_files.append(file)
    return csv_files


def create_df_from_csv(directory, csv_files):
    data_path = directory + '/'
    df = {}
    for file in csv_files:
        try:
            df[file] = pd.read_csv(data_path + file)
        except UnicodeDecodeError:
            df[file] = pd.read_csv(data_path + file, encoding="ISO-8859-1")
    return df
