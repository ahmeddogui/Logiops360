import pandas as pd
import numpy as np

def _detect_col(cols, candidates):
    norm = {c.lower().replace(" ", "_"): c for c in cols}
    for cand in candidates:
        key = cand.lower().replace(" ", "_")
        if key in norm:
            return norm[key]
    return None

def _read_customer_order_ids(engine) -> pd.Series:
    for table in ["clean_customer_orders", "raw_customer_orders"]:
        try:
            df_head = pd.read_sql(f"SELECT * FROM {table} LIMIT 0", engine)
            id_col = _detect_col(df_head.columns, ["order_id", "ordernumber", "order_number"])
            if id_col is None:
                continue
            df_ids = pd.read_sql(f"SELECT DISTINCT {id_col} AS order_id FROM {table}", engine)
            s = (
                df_ids["order_id"]
                .astype(str)
                .str.strip()
                .replace({"": np.nan})
                .dropna()
                .drop_duplicates()
            )
            if len(s) > 0:
                return s
        except Exception:
            pass
    return pd.Series(dtype=object)

def transform_supply_chain_problem(engine) -> pd.DataFrame:
    df_logistics = pd.read_sql("SELECT * FROM raw_supply_chain_problem", engine)
    df_clean_logistics = df_logistics.copy()
    df_clean_logistics.columns = [
        col.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        for col in df_clean_logistics.columns
    ]
    str_cols = df_clean_logistics.select_dtypes(include="object").columns
    for col in str_cols:
        df_clean_logistics[col] = (
            df_clean_logistics[col]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.upper()
        )
    order_id_col = _detect_col(df_clean_logistics.columns, ["order_id"])
    if order_id_col is not None:
        df_clean_logistics["order_id_src"] = df_clean_logistics[order_id_col]
        customer_order_ids = _read_customer_order_ids(engine)
        if len(customer_order_ids) > 0:
            unique_log_ids = (
                df_clean_logistics[order_id_col]
                .replace({"": np.nan})
                .dropna()
                .drop_duplicates()
                .sort_values()
                .tolist()
            )
            cust_ids_sorted = sorted(customer_order_ids.astype(str).tolist())
            if len(cust_ids_sorted) > 0:
                mapped = {}
                for i, lid in enumerate(unique_log_ids):
                    mapped[lid] = cust_ids_sorted[i % len(cust_ids_sorted)]
                df_clean_logistics[order_id_col] = df_clean_logistics[order_id_col].map(
                    lambda x: mapped.get(x, x)
                )
    df_clean_logistics = df_clean_logistics.drop_duplicates()
     
    assets = [f"TRUCK_{i}" for i in range(1, 11)]
    rng = np.random.default_rng()  # génère de l'aléatoire
    df_clean_logistics["asset_id"] = rng.choice(assets, size=len(df_clean_logistics))
 
    return df_clean_logistics
