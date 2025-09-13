import pandas as pd
from utils.db_utils import connect_db
import os
import csv
RAW_PATHS = {
    # Commandes
    "logiops360_back/Commandes/Data/Customer_Order.csv": "raw_customer_orders",
    "logiops360_back/Commandes/Data/Product.csv": "raw_products",
    "logiops360_back/Commandes/Data/Picking_Wave.csv": "raw_picking_wave",
    "logiops360_back/Commandes/Data/Supply_chain_logisitcs_problem.xlsx": {
        None: "raw_supply_chain_problem",   
        "WhCosts": "raw_whcosts",
        "WhCapacities": "raw_whcapacities",
    },
    "logiops360_back/Commandes/Data/supply_chain_data.xlsx": "raw_supply_chain_data",
 
    # Stockage
    "logiops360_back/Stockage/Data/Class_Based_Storage.csv": "raw_class_based_storage",
    "logiops360_back/Stockage/Data/Dedicated_Storage.csv": "raw_dedicated_storage",
    "logiops360_back/Stockage/Data/Hybrid_Storage.csv": "raw_hybrid_storage",
    "logiops360_back/Stockage/Data/Random_Storage.csv": "raw_random_storage",
    "logiops360_back/Stockage/Data/Storage_Location.csv": "raw_storage_location",
    "logiops360_back/Stockage/Data/Support_Points_Navigation.csv": "raw_support_points",

    # Transport
    "logiops360_back/Transport/Data/Monthly_Modal_Time_Series.csv": "raw_monthly_modal",
    "logiops360_back/Transport/Data/smart_logistics_dataset.csv": "raw_smart_logistics",
    "logiops360_back/Transport/Data/Supply chain logisitcs problem.xlsx": "raw_supply_chain_problem_2",
    "logiops360_back/Transport/Data/Transportation and Logistics Tracking Dataset..xlsx": "raw_transport_tracking"
}
 
 
def ingest_raw():
    engine = connect_db()
    for file_path, table_spec in RAW_PATHS.items():  # <-- ici: table_spec
        try:
            if file_path.endswith(".csv"):
                with open(file_path, "r", encoding="utf-8") as f:
                    dialect = csv.Sniffer().sniff(f.read(2048))
                    f.seek(0)
                    df = pd.read_csv(f, sep=dialect.delimiter)
                df.to_sql(table_spec, engine, if_exists="replace", index=False)
                print(f"[✓] {file_path} → {table_spec}")

            elif file_path.endswith(".xlsx"):
                if isinstance(table_spec, str):
                    df = pd.read_excel(file_path)
                    df.to_sql(table_spec, engine, if_exists="replace", index=False)
                    print(f"[✓] {file_path} (1ère feuille) → {table_spec}")
                elif isinstance(table_spec, dict):
                    xls = pd.ExcelFile(file_path)
                    for sheet_key, out_table in table_spec.items():
                        sheet_name = xls.sheet_names[0] if sheet_key is None else sheet_key
                        if sheet_name not in xls.sheet_names:
                            print(f"[!] Feuille '{sheet_name}' introuvable dans {file_path}. Feuilles: {xls.sheet_names}")
                            continue
                        df = pd.read_excel(xls, sheet_name=sheet_name)
                        df.to_sql(out_table, engine, if_exists="replace", index=False)
                        print(f"[✓] {file_path}::{sheet_name} → {out_table}")
                else:
                    raise ValueError("Spécification invalide pour .xlsx")
            else:
                raise ValueError("Format non pris en charge")

        except Exception as e:
            print(f"[✗] {file_path} → {table_spec} : {e}")

if __name__ == "__main__":
    ingest_raw()
 