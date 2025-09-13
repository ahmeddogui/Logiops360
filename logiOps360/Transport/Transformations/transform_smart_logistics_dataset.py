import pandas as pd

def transform_smart_logistics_dataset(engine) -> pd.DataFrame:
    # Lecture depuis la table raw
    df = pd.read_sql("SELECT * FROM raw_smart_logistics", engine)

    # Nettoyage des colonnes (snake_case)
    df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]

    # Conversion du timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Uniformiser le texte
    text_cols = ["shipment_status", "traffic_status", "logistics_delay_reason", "asset_id"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Remplacer None par UNKNOWN dans logistics_delay_reason
    if "logistics_delay_reason" in df.columns:
        df["logistics_delay_reason"] = df["logistics_delay_reason"].replace({"NONE": "UNKNOWN", "NAN": "UNKNOWN"})

    # Conversion des colonnes numériques
    num_cols = df.select_dtypes(include=["float64", "int64"]).columns
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Suppression des valeurs aberrantes pour la température
    if "temperature" in df.columns:
        df = df[(df["temperature"] >= -50) & (df["temperature"] <= 60)]

    # Suppression des doublons
    df = df.drop_duplicates()

    return df
