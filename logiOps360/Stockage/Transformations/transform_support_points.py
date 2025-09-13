import pandas as pd
import numpy as np
import re
import csv

def _parse_semicolon_line(s: str):
    row = next(csv.reader([s], delimiter=';', quotechar='"'), [])
    if not row:
        return "", ""
    label = row[0].strip() if len(row) >= 1 else ""
    points = row[1] if len(row) >= 2 else ""
    if len(row) > 2:
        points = ";".join(row[1:])
    return label, points

def transform_support_points(engine) -> pd.DataFrame:
    df_raw = pd.read_sql("SELECT * FROM raw_support_points", engine)
    if {"labels", "points_specified"}.issubset(set(df_raw.columns.str.lower())):
        cols = {c.lower(): c for c in df_raw.columns}
        df_use = df_raw[[cols["labels"], cols["points_specified"]]].rename(columns={cols["labels"]: "labels", cols["points_specified"]: "points_specified"})
    else:
        first_col = df_raw.columns[0]
        parsed = df_raw[first_col].astype(str).apply(_parse_semicolon_line)
        df_use = pd.DataFrame(parsed.tolist(), columns=["labels", "points_specified"])
    corrected = []
    for _, row in df_use.iterrows():
        label = str(row["labels"]) if pd.notna(row["labels"]) else "UNLABELED"
        label_clean = label.strip().upper()[:50]
        points_str = str(row["points_specified"]) if pd.notna(row["points_specified"]) else ""
        points_norm = re.sub(r"[;,|\s]+", ",", points_str.strip())
        nums = re.findall(r"[-+]?\d*\.?\d+", points_norm)
        if len(nums) < 3:
            nums += ["0"] * (3 - len(nums))
            is_valid = False
        else:
            is_valid = True
        try:
            x, y, z = map(float, nums[:3])
        except Exception:
            x, y, z = 0.0, 0.0, 0.0
            is_valid = False
        corrected.append(
            {
                "label": label_clean,
                "x_coord": x,
                "y_coord": y,
                "z_coord": z,
                "is_valid": is_valid,
            }
        )
    df_corrected = pd.DataFrame(corrected)
    if df_corrected.empty:
        return pd.DataFrame(columns=["label", "x_coord", "y_coord", "z_coord", "norm"])
    df_corrected["norm"] = np.sqrt(df_corrected["x_coord"] ** 2 + df_corrected["y_coord"] ** 2 + df_corrected["z_coord"] ** 2)
    type_map = {
        "label": "category",
        "x_coord": "float32",
        "y_coord": "float32",
        "z_coord": "float32",
        "norm": "float32",
        "is_valid": "bool",
    }
    for c, t in type_map.items():
        if c in df_corrected.columns:
            try:
                df_corrected[c] = df_corrected[c].astype(t)
            except Exception:
                pass
    return df_corrected[["label", "x_coord", "y_coord", "z_coord", "norm"]]
