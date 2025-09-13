import pandas as pd

def transform_picking_wave(engine) -> pd.DataFrame:
    df_wave = pd.read_sql("SELECT * FROM raw_picking_wave", engine)
    s = df_wave.iloc[:, 0].astype(str)
    split = s.str.split(";", expand=True)
    if split.shape[1] == 6:
        split.columns = ["waveNumber", "reference", "Size (US)", "quantityToPick (units)", "locations", "operator"]
    elif split.shape[1] == 5:
        split.columns = ["waveNumber", "reference", "Size (US)", "quantityToPick (units)", "operator"]
        split["locations"] = None
    else:
        raise ValueError(f"Unexpected number of columns after split: {split.shape[1]}")
    df_wave_clean = split.drop_duplicates().copy()
    for col in ["reference", "operator"]:
        df_wave_clean[col] = df_wave_clean[col].astype(str).str.strip()
    df_wave_clean.columns = [col.strip().lower().replace(" ", "_").replace("(", "").replace(")", "") for col in df_wave_clean.columns]
    df_wave_clean.rename(columns={"wavenumber": "wave_number", "size_us": "size_us", "quantitytopick_units": "quantity_to_pick_units"}, inplace=True)
    df_wave_clean["wave_number"] = pd.to_numeric(df_wave_clean["wave_number"], errors="coerce")
    df_wave_clean["size_us"] = pd.to_numeric(df_wave_clean["size_us"], errors="coerce")
    df_wave_clean["quantity_to_pick_units"] = pd.to_numeric(df_wave_clean["quantity_to_pick_units"], errors="coerce")
    df_wave_clean = df_wave_clean.dropna(subset=["reference", "quantity_to_pick_units"])
    df_wave_clean = df_wave_clean[df_wave_clean["quantity_to_pick_units"].between(0, 10000)]
    df_wave_clean["reference"] = df_wave_clean["reference"].str.upper().str.replace("-", "", regex=False).str.strip()
    df_wave_clean = df_wave_clean.drop_duplicates(subset=["wave_number", "reference", "operator"])
    df_wave_clean = df_wave_clean.drop(columns=["locations"])
    return df_wave_clean
