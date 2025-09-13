import pandas as pd
import csv
from io import StringIO

def _parse_single_col_line_dedicated(raw_line: str):
    """
    Parse une ligne 'single column' du dedicated storage.
    Retourne: location, xyz_class, list[(ref, qty_float, util_float_or_None)]
    """
    cells = next(csv.reader(StringIO(raw_line), delimiter=';', quotechar='"'), [])
    if not cells or len(cells) < 2:
        return None

    # ignorer l'en-tête
    if cells[0].strip().lower() == "location":
        return None

    location = (cells[0] or "").strip()
    xyz_class = (cells[1] or "").strip()

    triples = []
    for tok in cells[2:]:
        if not tok:
            continue
        # tok peut être: 'CODE;QTE' ou 'CODE;QTE;UTIL'
        # Comme on a déjà split la ligne principale, ici 'tok' contient encore un sous-texte avec ;.
        parts = tok.split(';')
        if len(parts) < 2:
            continue

        ref = (parts[0] or "").strip().upper()

        qty = (parts[1] or "").strip().replace(',', '.')
        try:
            qty_val = float(qty)
        except ValueError:
            continue

        util_val = None
        if len(parts) > 2 and parts[2] is not None and parts[2] != "":
            util = parts[2].strip().replace(',', '.')
            try:
                util_val = float(util)
            except ValueError:
                util_val = None

        if ref:
            triples.append((ref, qty_val, util_val))

    return location, xyz_class, triples


def transform_dedicated_storage(engine) -> pd.DataFrame:
    # Charger la table brute
    df_raw = pd.read_sql("SELECT * FROM raw_dedicated_storage", engine)

    melted = []

    if "Location" in df_raw.columns and "XYZCOD" in df_raw.columns:
        # ===========================
        # CAS 1 : données déjà en colonnes
        # ===========================
        for _, row in df_raw.iterrows():
            location = (row.get("Location") or "").strip()
            xyz_class = (row.get("XYZCOD") or "").strip()

            for i in range(1, 19):
                col = str(i)
                if col not in df_raw.columns:
                    continue

                material_info = row[col]
                if pd.isna(material_info):
                    continue

                s = str(material_info)
                # s attendu: 'REF;QTE' ou 'REF;QTE;UTIL'
                parts = [p.strip() for p in s.split(';')]
                if len(parts) < 2:
                    continue

                ref = (parts[0] or "").upper()
                qty_txt = (parts[1] or "").replace(',', '.')
                try:
                    qty_val = float(qty_txt)
                except ValueError:
                    continue

                util_val = None
                if len(parts) > 2 and parts[2] != "":
                    util_txt = parts[2].replace(',', '.')
                    try:
                        util_val = float(util_txt)
                    except ValueError:
                        util_val = None

                if ref:
                    melted.append({
                        "location": location,
                        "class": xyz_class.upper() if xyz_class else None,
                        "referenceproduit": ref,
                        "quantity": qty_val,
                        "utilization_rate": util_val,
                        "storage_type": "dedicated",
                    })

    else:
        # ===========================
        # CAS 2 : une seule colonne texte
        # ===========================
        single_col = df_raw.columns[0]
        for raw in df_raw[single_col].astype(str):
            parsed = _parse_single_col_line_dedicated(raw)
            if not parsed:
                continue
            location, xyz_class, triples = parsed
            for ref, qty_val, util_val in triples:
                melted.append({
                    "location": location,
                    "class": xyz_class.upper() if xyz_class else None,
                    "referenceproduit": ref,
                    "quantity": qty_val,
                    "utilization_rate": util_val,
                    "storage_type": "dedicated",
                })

    df_clean = pd.DataFrame(melted, columns=[
        "location", "class", "referenceproduit", "quantity", "utilization_rate", "storage_type"
    ])

    # Nettoyage final
    if not df_clean.empty:
        df_clean["referenceproduit"] = df_clean["referenceproduit"].astype("string").str.strip().str.upper()
        df_clean["class"] = df_clean["class"].astype("string").str.strip().str.upper()
        # conversions sûres (si déjà float, ne change rien)
        df_clean["quantity"] = pd.to_numeric(df_clean["quantity"], errors="coerce")
        df_clean["utilization_rate"] = pd.to_numeric(df_clean["utilization_rate"], errors="coerce")
        # supprimer lignes incomplètes
        df_clean = df_clean.dropna(subset=["referenceproduit", "class", "quantity"])

    return df_clean
