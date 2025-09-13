import pandas as pd

def transform_transportation_and_logistics(engine) -> pd.DataFrame:
    # Lecture de la table raw depuis PostgreSQL
    df = pd.read_sql("SELECT * FROM raw_transport_tracking", engine)

    # Nettoyage des noms de colonnes
    df.columns = [col.lower().strip().replace(" ", "_").replace("/", "_") for col in df.columns]

    # Conversion des colonnes datetime
    datetime_cols = ["bookingid_date", "data_ping_time", "planned_eta", "actual_eta", "trip_start_date", "trip_end_date"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Normaliser le texte
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip().str.upper()
        df[col] = df[col].replace({"NAN": "UNKNOWN", "NONE": "UNKNOWN"})

    # Conversion des colonnes numériques
    num_cols = df.select_dtypes(include=["float64", "int64"]).columns
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Suppression des doublons
    df = df.drop_duplicates()

    # Suppression des lignes sans identifiants critiques
    df = df.dropna(subset=["bookingid", "vehicle_no", "trip_start_date", "transportation_distance_in_km"])

    # Supprimer les colonnes avec plus de 80 % de valeurs manquantes
    seuil_null = 0.8
    df = df.loc[:, df.isnull().mean() < seuil_null]

    return df
