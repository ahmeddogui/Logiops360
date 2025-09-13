import pandas as pd
import numpy as np
 
def transform_supply_chain_problem(engine) -> pd.DataFrame:
    # Lecture de la table RAW
    df = pd.read_sql("SELECT * FROM raw_supply_chain_problem_2", engine)
 
    # Standardiser les noms de colonnes
    df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
 
    # Conversion date (ou génération aléatoire si nécessaire)
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
 
        # Vérifier si les dates sont toutes identiques ou nulles
        if df["order_date"].nunique() <= 1:
            # Générer des dates aléatoires entre 2013-01-01 et 2024-12-31
            start_date = pd.to_datetime("2013-01-01")
            end_date = pd.to_datetime("2024-12-31")
            df["order_date"] = pd.to_datetime(np.random.randint(
                start_date.value // 10**9,
                end_date.value // 10**9,
                size=len(df)
            ), unit='s')
 
    else:
        # Si la colonne n'existe pas, on la crée avec des dates aléatoires
        start_date = pd.to_datetime("2013-01-01")
        end_date = pd.to_datetime("2024-12-31")
        df["order_date"] = pd.to_datetime(np.random.randint(
            start_date.value // 10**9,
            end_date.value // 10**9,
            size=len(df)
        ), unit='s')
 
    # Nettoyage texte
    str_cols = ["origin_port", "carrier", "service_level", "customer", "plant_code", "destination_port"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
 
    # Sécurisation des types numériques
    num_cols = ["order_id", "tpt", "ship_ahead_day_count", "ship_late_day_count", "product_id", "unit_quantity", "weight"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
 
    # Supprimer les doublons
    df = df.drop_duplicates()
 
    return df
 