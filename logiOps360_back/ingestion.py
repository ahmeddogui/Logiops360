import pandas as pd
from utils.db_utils import connect_db
import os
import csv
RAW_PATHS = {
 
    "Stockage/Data/Class_Based_Storage.csv": "raw_class_based_storage",
    "Stockage/Data/Dedicated_Storage.csv": "raw_dedicated_storage",
    "Stockage/Data/Hybrid_Storage.csv": "raw_hybrid_storage",
    "Stockage/Data/Random_Storage.csv": "raw_random_storage",
    "Stockage/Data/Storage_Location.csv": "raw_storage_location",
    "Stockage/Data/Support_Points_Navigation.csv": "raw_support_points",
}
def ingest_raw():
    engine = connect_db()
    for file_path, table_name in RAW_PATHS.items():  
        try:
            if file_path.endswith(".csv"):
                with open(file_path, 'r', encoding='utf-8') as f:
                 dialect = csv.Sniffer().sniff(f.read(2048))
                 f.seek(0)
                 df = pd.read_csv(f, sep=dialect.delimiter)
            elif file_path.endswith(".xlsx"):
             df = pd.read_excel(file_path)
            else:
                raise ValueError("Format non pris en charge")
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"{file_path} → {table_name}")
        except Exception as e:
            print(f"{file_path} → {table_name} : {e}")
 
if __name__ == "__main__":
    ingest_raw()
 