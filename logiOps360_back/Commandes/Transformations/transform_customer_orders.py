import pandas as pd
from sqlalchemy import text

# cibles clean -> variantes possibles en RAW (insensibles à la casse/espaces)
COLMAP = {
    "ordernumber":    ["ordernumber", "order_number", "order id", "orderid"],
    "codcustomer":    ["codcustomer", "customer_code", "customerid", "customer_id"],
    "reference":      ["reference", "sku", "product_ref", "ref"],
    "quantity_units": ["quantity_units", "qty", "quantity", "units", "quantity (units)"],
    "creationdate":   ["creationdate", "created_at", "order_date", "creation_date", "creation date"],
    # colonnes annexes possibles dans la source
    "waveNumber":     ["wavenumber", "wave_number", "wave", "waveid"],
    "operator":       ["operator", "picker", "employee", "user"],
    "orderToCollect": ["ordertocollect", "order_to_collect", "to_collect"],
    "Size (US)":      ["size (us)", "size_us", "size"],
}

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace("\ufeff", "", regex=False)
        .str.replace(r"\s+", " ", regex=True)
    )
    return df

def _map_cols(df: pd.DataFrame, colmap: dict) -> pd.DataFrame:
    """Mappe de façon tolérante les colonnes RAW vers les noms cibles."""
    cols_lookup = {c.lower(): c for c in df.columns}
    rename = {}
    for target, candidates in colmap.items():
        found = None
        for c in candidates:
            key = c.lower()
            if key in cols_lookup:
                found = cols_lookup[key]
                break
        if found:
            rename[found] = target
    return df.rename(columns=rename)

def transform_customer_orders(engine) -> pd.DataFrame:
    # Charge le RAW
    df_raw = pd.read_sql_query("SELECT * FROM raw_customer_orders", engine)
    df_raw = _normalize_cols(df_raw)

    # CAS 1 : table déjà multicolonnes -> on mappe
    if df_raw.shape[1] > 1:
        df = _map_cols(df_raw, COLMAP)

    # CAS 2 : table mono-colonne -> on split sur ';'
    else:
        # split sûr avec n=8 (=> 9 colonnes max)
        parts = df_raw.iloc[:, 0].astype(str).str.split(";", expand=True, n=8)
        if parts.shape[1] != 9:
            raise ValueError(f"RAW mono-colonne mais split a donné {parts.shape[1]} colonnes (attendu 9).")
        parts.columns = [
            "codCustomer", "orderNumber", "orderToCollect", "Reference",
            "Size (US)", "quantity (units)", "creationDate", "waveNumber", "operator"
        ]
        df = parts

    # Normalise & (re)mappe une dernière fois pour harmoniser les noms
    df = _normalize_cols(df)
    df = _map_cols(df, COLMAP)

    # Nettoyage / cast
    if "reference" in df.columns:
        df["reference"] = df["reference"].astype(str).str.upper().str.replace("-", "", regex=False).str.strip()

    if "ordernumber" in df.columns:
        # garde string (ton simulateur caste en str)
        df["ordernumber"] = df["ordernumber"].astype(str).str.strip()

    if "codcustomer" in df.columns:
        df["codcustomer"] = df["codcustomer"].astype(str).str.strip()

    if "quantity_units" in df.columns:
        df["quantity_units"] = pd.to_numeric(df["quantity_units"], errors="coerce").fillna(0).astype(float)

    if "creationdate" in df.columns:
        # parse tolérant; pas de forçage année 2025 ici
        df["creationdate"] = pd.to_datetime(df["creationdate"], errors="coerce", utc=True)

    # Filtre lignes minimales valides
    required = ["ordernumber", "codcustomer", "reference", "quantity_units", "creationdate"]
    for c in required:
        if c not in df.columns:
            raise KeyError(f"Colonne requise manquante après mapping: {c}. Colonnes présentes: {list(df.columns)}")

    df = df.dropna(subset=["ordernumber", "reference", "creationdate"])
    df = df.drop_duplicates(subset=["ordernumber", "reference", "creationdate"])

    # Renvoie EXACTEMENT ce que consomme le simulateur
    return df[required]
