import pandas as pd
import csv
from io import StringIO

def _parse_single_col_line(raw_line: str):
    """
    Parse une ligne brute 'single column' et retourne:
    location (str), abc_class (str), list[(ref, qty_float)]
    """
    # parser CSV robuste: delimiter=';' + quotechar='"'
    cells = next(csv.reader(StringIO(raw_line), delimiter=';', quotechar='"'), [])
    if not cells or len(cells) < 2:
        return None

    # ignorer l’en-tête "Location;ABCCOD;1;...;18"
    if cells[0].strip().lower() == "location":
        return None

    location = (cells[0] or "").strip()
    abc_class = (cells[1] or "").strip()

    pairs = []
    for tok in cells[2:]:
        if not tok:
            continue
        # ex: tok == '8551FLX;15.0' (les guillemets externes sont déjà retirés)
        if ';' not in tok:
            continue
        ref, qty = tok.split(';', 1)
        ref = (ref or "").strip().upper()
        qty = (qty or "").strip().replace(',', '.')  # décimales avec virgule
        try:
            qty_val = float(qty)
        except ValueError:
            continue
        if ref and qty_val is not None:
            pairs.append((ref, qty_val))

    return location, abc_class, pairs


def transform_class_based_storage(engine) -> pd.DataFrame:
    # 1) Charger la table brute
    df_raw = pd.read_sql("SELECT * FROM raw_class_based_storage", engine)

    melted = []

    if "Location" in df_raw.columns and "ABCCOD" in df_raw.columns:
        # ===========================
        # CAS 1 : données déjà en colonnes
        # ===========================
        for _, row in df_raw.iterrows():
            location = (row["Location"] or "").strip()
            abc_class = (row["ABCCOD"] or "").strip()

            for i in range(1, 19):
                col = str(i)
                if col not in df_raw.columns:
                    continue
                material_info = row[col]
                if pd.isna(material_info):
                    continue

                # chaque cellule ressemble à 'CODE;QTE' (éventuellement avec guillemets dans la source)
                s = str(material_info)
                if ';' not in s:
                    continue
                ref, qty = s.split(';', 1)
                ref = (ref or "").strip().upper()
                qty = (qty or "").strip().replace(',', '.')
                try:
                    qty_val = float(qty)
                except ValueError:
                    continue

                melted.append({
                    "location": location,
                    "class": abc_class.upper() if abc_class else None,
                    "referenceproduit": ref,
                    "quantity": qty_val,
                    "storage_type": "class_based",
                })

    else:
        single_col = df_raw.columns[0]
        for raw in df_raw[single_col].astype(str):
            parsed = _parse_single_col_line(raw)
            if not parsed:
                continue
            location, abc_class, pairs = parsed
            for ref, qty_val in pairs:
                melted.append({
                    "location": location,
                    "class": abc_class.upper() if abc_class else None,
                    "referenceproduit": ref,
                    "quantity": qty_val,
                    "storage_type": "class_based",
                })

    df_clean = pd.DataFrame(melted, columns=[
        "location", "class", "referenceproduit", "quantity", "storage_type"
    ])

    # Nettoyage final
    if not df_clean.empty:
        df_clean["referenceproduit"] = df_clean["referenceproduit"].str.strip().str.upper()
        if "class" in df_clean.columns:
            df_clean["class"] = df_clean["class"].astype("string").str.strip().str.upper()
        # supprimer lignes incomplètes
        df_clean = df_clean.dropna(subset=["referenceproduit", "quantity"])

    return df_clean
