import pandas as pd
import numpy as np

def ensure_datetime(df, col):
    if col in df.columns:
        if not np.issubdtype(df[col].dtype, np.datetime64):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        return col
    for c in ["creation_date", "creationdate", "date", "order_date"]:
        if c in df.columns:
            if not np.issubdtype(df[c].dtype, np.datetime64):
                df[c] = pd.to_datetime(df[c], errors="coerce")
            return c
    raise ValueError("No date column found")

def weekly_agg(df, date_col, ref_col, qty_col):
    df = df[[date_col, ref_col, qty_col]].dropna(subset=[date_col, ref_col, qty_col])
    df["week"] = df[date_col].dt.to_period("W-MON").dt.start_time
    g = df.groupby([ref_col, "week"], as_index=False)[qty_col].sum()
    g = g.rename(columns={qty_col: "qty"})
    g["qty"] = g["qty"].fillna(0).round().clip(lower=0).astype(int)
    return g

def add_lags(df, ref_col, lags=(1,2,3,4)):
    df = df.sort_values(["reference","week"])
    for L in lags:
        df[f"lag_{L}"] = df.groupby(ref_col)["qty"].shift(L)
    return df

def add_time_feats(df):
    df["dow"] = df["week"].dt.dayofweek
    df["month"] = df["week"].dt.month
    df["year"] = df["week"].dt.year
    df["weekofyear"] = df["week"].dt.isocalendar().week.astype(int)
    return df
