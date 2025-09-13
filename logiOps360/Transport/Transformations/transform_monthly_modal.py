import pandas as pd
import numpy as np

def transform_monthly_modal(engine) -> pd.DataFrame:
    # Lecture
    df = pd.read_sql("SELECT * FROM raw_monthly_modal", engine)

    # Nettoyage noms colonnes
    df.columns = [
        col.lower()
           .replace(" ", "_")
           .replace("(", "")
           .replace(")", "")
           .replace("/", "_")
           .replace("-", "_")
           .replace("__", "_")
        for col in df.columns
    ]

    # Filtrage colonnes trop vides
    seuil_null = 0.8
    df = df.loc[:, df.isnull().mean() < seuil_null]

    # Colonnes inutiles
    colonnes_inutiles = [
        "primary_uza_sq_miles", "primary_uza_population",
        "service_area_sq_miles", "service_area_population",
        "mo_yr", "month_year_timestamp",
        "non_major_physical_assaults_on_operators",
        "non_major_non_physical_assaults_on_operators",
        "non_major_physical_assaults_on_other_transit_workers",
        "non_major_non_physical_assaults_on_other_transit_workers",
        "major_physical_assaults_on_operators",
        "major_non_physical_assaults_on_operators",
        "major_physical_assaults_on_other_transit_workers",
        "major_non_physical_assaults_on_other_transit_workers",
        "total_assaults_on_transit_workers"
    ]
    df = df.drop(columns=[c for c in colonnes_inutiles if c in df.columns])

    # Nettoyage catégorielles
    cat_cols = df.select_dtypes(include="object").columns
    for c in cat_cols:
        df[c] = df[c].astype(str).str.strip().str.upper()

    # Numériques
    num_cols = df.select_dtypes(include=["float64", "int64"]).columns
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Date depuis mo_yr si présent
    if "mo_yr" in df.columns:
        df["date"] = pd.to_datetime(df["mo_yr"].astype(str), format="%Y%m%d", errors="coerce")

    # Doublons et valeurs incohérentes
    df = df.drop_duplicates()
    if "vehicle_revenue_hours" in df.columns:
        df = df[df["vehicle_revenue_hours"] >= 0]

    # ---- AJOUT Asset_ID aléatoire ----
    assets = [f"Truck_{i}" for i in range(1, 11)]
    rng = np.random.default_rng()  # génère de l'aléatoire
    df["asset_id"] = rng.choice(assets, size=len(df))

    return df
