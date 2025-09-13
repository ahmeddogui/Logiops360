import requests
import pandas as pd
import sys
from pathlib import Path
import subprocess

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.db_utils import connect_db
from Commandes.Transformations.transform_customer_orders import transform_customer_orders

engine = connect_db()

def fetch_and_store_new_orders():
    response = requests.get("http://127.0.0.1:5000/new_orders")
    new_orders = pd.DataFrame(response.json())
    if not new_orders.empty:
        new_orders["raw_line"] = new_orders.apply(
            lambda row: f"{row['codCustomer']};{row['orderNumber']};{row['orderToCollect']};"
                        f"{row['Reference']};{row['Size (US)']};{row['quantity (units)']};"
                        f"{row['creationDate']};{row['waveNumber']};{row['operator']}",
            axis=1
        )
        column_name_pg = "codCustomer;orderNumber;orderToCollect;Reference;Size (US);quantity (units);creationDate;waveNumber;operator"
        new_orders = new_orders.rename(columns={"raw_line": column_name_pg})[[column_name_pg]]
        new_orders.to_sql("raw_customer_orders", con=engine, if_exists="append", index=False)
        print(f"{len(new_orders)} nouvelles lignes brutes insérées dans raw_customer_orders.")
    else:
        print("Aucune nouvelle commande à insérer aujourd'hui.")

def insert_clean_orders(engine, df_clean):
    existing_orders = pd.read_sql("SELECT ordernumber FROM clean_customer_orders", con=engine)
    existing_orders_set = set(existing_orders["ordernumber"].astype(int))

    df_new = df_clean[~df_clean["ordernumber"].isin(existing_orders_set)]

    if not df_new.empty:
        df_new.to_sql("clean_customer_orders", con=engine, if_exists="append", index=False)
        print(f"{len(df_new)} nouvelles lignes propres insérées dans clean_customer_orders.")
    else:
        print("ℹAucune nouvelle ligne propre à insérer.")

def etl_logiops():
    print("Début du pipeline ETL LogiOps360")

    # 1. Ingestion 
    fetch_and_store_new_orders()

    # 2. Nettoyage
    df_clean = transform_customer_orders(engine)
    df_clean = transform_customer_orders(engine)
    insert_clean_orders(engine, df_clean)

    # 3. Mise à jour de la vue SQL
    subprocess.run(["python", "../Commandes/Analysis/sql_relations.py"])

    print("Pipeline ETL LogiOps360 exécuté avec succès.")

if __name__ == "__main__":
    etl_logiops()
