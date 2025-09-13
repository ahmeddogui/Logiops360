import pandas as pd
import csv
from io import StringIO

def _parse_single_col_line_hybrid(raw_line: str):
    """
    Parse une ligne 'single column' du hybrid storage.
    Retourne: location, xyzcod, list[(material_upper, qty_float, source_index_str)]
    """
    cells = next(csv.reader(StringIO(raw_line), delimiter=';', quotechar='"'), [])
    if not cells or len(cells) < 2:
        return None

    if cells[0].strip().lower() == "location":
        return None

    location = (cells[0] or "").strip()
    xyzcod   = (cells[1] or "").strip()
    triples  = []

    for idx, tok in enumerate(cells[2:], start=1):
        if not tok:
            continue
        parts = [p.strip() for p in tok.split(';')]
        if len(parts) < 2:
            continue

        material = (parts[0] or "").strip().upper()
        qty_txt  = (parts[1] or "").strip().replace(',', '.')
        try:
            qty_val = float(qty_txt)
        except ValueError:
            continue

        pos_label = f"POS-{idx:02d}"
        triples.append((material, qty_val, pos_label))

    return location, xyzcod, triples


def transform_hybrid_storage(engine) -> pd.DataFrame:
    """
    Transforme les données de stockage hybride :
    - Compatible colonnes (Location, XYZCOD, 1..18) ET 'single column'
    - Parsing robuste avec ; et guillemets
    - Nettoyage + calcul des métriques
    """
    # Chargement
    df_raw = pd.read_sql("SELECT * FROM raw_hybrid_storage", engine)

    melted_data = []

    if "Location" in df_raw.columns and "XYZCOD" in df_raw.columns:
        # ===========================
        # CAS 1 : données déjà en colonnes
        # ===========================
        for _, row in df_raw.iterrows():
            location = (row.get("Location") or "").strip()
            xyzcod   = (row.get("XYZCOD") or "").strip()

            for i in range(1, 19):
                col_name = str(i)
                if col_name not in df_raw.columns:
                    continue

                cell_content = row[col_name]
                if pd.isna(cell_content):
                    continue

                s = str(cell_content)
                try:
                    parts = [p.strip() for p in s.split(';') if p is not None and str(p).strip() != ""]
                    if len(parts) >= 2:
                        material = (parts[0] or "").upper()
                        qty_txt  = (parts[1] or "").replace(',', '.')
                        quantity = float(qty_txt)

                        melted_data.append({
                            "location": location,
                            "xyzcod": xyzcod,
                            "position": f"POS-{i:02d}",
                            "material": material,
                            "quantity": quantity,
                            "source_column": col_name,
                        })
                except (ValueError, AttributeError):
                    continue
    else:
        # ===========================
        # CAS 2 : une seule colonne texte
        # ===========================
        single_col = df_raw.columns[0]
        for raw in df_raw[single_col].astype(str):
            parsed = _parse_single_col_line_hybrid(raw)
            if not parsed:
                continue
            location, xyzcod, triples = parsed
            for material, quantity, pos_label in triples:
                melted_data.append({
                    "location": location,
                    "xyzcod": xyzcod,
                    "position": pos_label,
                    "material": material,
                    "quantity": quantity,
                    "source_column": pos_label.replace("POS-", ""),  # info indicative
                })

    # DataFrame transformé
    df_clean = pd.DataFrame(melted_data)

    if df_clean.empty:
        return df_clean

    # =======================
    # 2. Nettoyage avancé
    # =======================
    df_clean["location"] = (
        df_clean["location"]
        .astype(str).str.strip().str.upper()
        .str.replace(r"[^A-Z0-9\-_]", "", regex=True)
    )
    df_clean["xyzcod"] = (
        df_clean["xyzcod"]
        .astype(str).str.strip().str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
    )
    df_clean["material"] = df_clean["material"].astype(str).str.strip().str.upper()

    # convertir quantité (au cas où)
    df_clean["quantity"] = pd.to_numeric(
        df_clean["quantity"], errors="coerce"
    )

    # filtrage des entrées invalides
    df_clean = df_clean[
        (df_clean["location"].str.len() > 0)
        & (df_clean["xyzcod"].str.len() > 0)
        & (df_clean["material"].str.len() > 0)
        & (df_clean["quantity"] > 0)
    ].copy()

    if df_clean.empty:
        return df_clean

    # =======================
    # 3. Calcul des métriques
    # =======================
    df_clean["is_duplicate"] = df_clean.duplicated(
        subset=["location", "position", "material"], keep=False
    )

    stats_df = df_clean.groupby(["location", "xyzcod"]).agg(
        total_items=("material", "count"),
        total_quantity=("quantity", "sum"),
        unique_materials=("material", "nunique"),
        positions_used=("position", "nunique"),
    ).reset_index()

    df_clean = df_clean.merge(stats_df, on=["location", "xyzcod"], how="left")

    # =======================
    # 4. Optimisation du typage
    # =======================
    dtype_mapping = {
        "location": "category",
        "xyzcod": "category",
        "position": "category",
        "material": "category",
        "quantity": "float32",
        "source_column": "category",
        "is_duplicate": "bool",
        "total_items": "uint16",
        "total_quantity": "float32",
        "unique_materials": "uint16",
        "positions_used": "uint8",
    }
    for col, dt in dtype_mapping.items():
        if col in df_clean.columns:
            try:
                df_clean[col] = df_clean[col].astype(dt)
            except Exception:
                # fallback silencieux si conversion impossible
                pass

    # =======================
    # 5. Réorganisation des colonnes
    # =======================
    final_columns = [
        "location", "xyzcod", "position", "material", "quantity",
        "total_items", "total_quantity", "unique_materials", "positions_used",
        "is_duplicate", "source_column",
    ]
    return df_clean[[c for c in final_columns if c in df_clean.columns]]
