import os
import sys
import importlib
from pathlib import Path
import pandas as pd
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.db_utils import connect_db

 
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
TRANSFORM_FUNCS = [
    "Commandes.Transformations.transform_customer_orders.transform_customer_orders",
    "Commandes.Transformations.transform_product.transform_product",
    "Commandes.Transformations.transform_supply_chain_problem.transform_supply_chain_problem"
]
 
TABLE_NAME_OVERRIDES = {
    "transform_supply_chain_problem": "clean_supply_chain_problem"
}
 
 
def resolve_callable(dotted_path: str):
    module_path, func_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)
 
 
def main():
    print(">>> MAIN COMMANDES LANCÉ")
    engine = connect_db()
    for dotted_path in TRANSFORM_FUNCS:
        transform_fn = resolve_callable(dotted_path)
        fn_name = transform_fn.__name__
        table_name = TABLE_NAME_OVERRIDES.get(
            fn_name, f"clean_{fn_name.replace('transform_', '')}"
        )
        try:
            with engine.connect() as conn:
                df = transform_fn(conn)
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                print(f"{table_name} : {len(df)} lignes insérées")
        except Exception as e:
            print(f"{table_name} : erreur - {e}")
    print("Transformations COMMANDES terminées.")
 
 


 
 
def transform_product(engine) -> pd.DataFrame:
    df_product = pd.read_sql("SELECT * FROM raw_products", con=engine)
 
    # Split si la table est mono-colonne avec des ';'
    if df_product.shape[1] == 1 and df_product.iloc[0, 0].count(";") > 0:
        df_product = df_product.iloc[:, 0].str.split(";", expand=True)
 
    # Si df_product a maintenant 3 colonnes, on peut renommer
    if df_product.shape[1] == 3:
        df_product.columns = ["Reference", "ABCCOD", "Sector"]
 
    # Normalisation des noms
    df_product.columns = [
        col.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        for col in df_product.columns
    ]
 
    # Nettoyage
    df_clean_product = df_product.drop_duplicates()
    str_cols = df_clean_product.select_dtypes(include='object').columns
    for col in str_cols:
        df_clean_product[col] = df_clean_product[col].astype(str).str.strip()
 
    df_clean_product = df_clean_product.dropna(how='all')
    df_clean_product = df_clean_product.dropna(subset=['reference'])
    df_clean_product['reference'] = df_clean_product['reference'].str.upper().str.replace("-", "").str.strip()
 
    return df_clean_product
 
 
if __name__ == "__main__":
    main()