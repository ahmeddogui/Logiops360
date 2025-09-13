import pandas as pd
import numpy as np

def transform_random_storage(engine) -> pd.DataFrame:
    """
    Transformation du stockage aléatoire avec colonnes harmonisées,
    suppression de certaines colonnes et renommage demandé.
    """
    df_raw = pd.read_sql("SELECT * FROM raw_random_storage", engine)

    melted_data = []

    for _, row in df_raw.iterrows():
        location = row['originalLocation']

        for i in range(1, 19):
            col_name = f'col_{i}'
            cell_content = row[col_name]

            if pd.notna(cell_content) and isinstance(cell_content, str):
                try:
                    parts = [p.strip() for p in cell_content.split(';') if p.strip()]

                    if len(parts) >= 2:
                        referenceproduit = parts[0].upper()
                        quantity = float(parts[1])

                        melted_data.append({
                            'location': location,
                            'position': f'POS-{i:02d}',
                            'referenceproduit': referenceproduit,
                            'quantity': quantity,
                            'source_column': col_name,
                            'storage_type': 'random'
                        })
                except (ValueError, AttributeError):
                    continue

    df_clean = pd.DataFrame(melted_data)

    df_clean['location'] = (
        df_clean['location']
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r'[^A-Z0-9\-_]', '', regex=True)
    )

    df_clean = df_clean[
        (df_clean['location'].str.len() > 0) &
        (df_clean['referenceproduit'].str.len() > 0) &
        (df_clean['quantity'] > 0)
    ]

    # Suppression des colonnes position, is_duplicate, source_column, total_items plus tard, donc on les conserve pour le moment pour le calcul

    df_clean['is_duplicate'] = df_clean.duplicated(
        subset=['location', 'position', 'referenceproduit'],
        keep=False
    )

    stats_df = df_clean.groupby('location').agg(
        total_items=('referenceproduit', 'count'),
        total_quantity=('quantity', 'sum'),
        unique_materials=('referenceproduit', 'nunique')
    ).reset_index()

    df_clean = pd.merge(df_clean, stats_df, on='location', how='left')

    # Maintenant suppression des colonnes indésirables
    df_clean = df_clean.drop(columns=['position', 'is_duplicate', 'source_column', 'total_items'])

    # Renommage de la colonne
    df_clean = df_clean.rename(columns={'unique_materials': 'produituniqueposition'})

    dtype_mapping = {
        'location': 'category',
        'referenceproduit': 'category',
        'quantity': 'float32',
        'storage_type': 'category',
        'total_quantity': 'float32',
        'produituniqueposition': 'uint16'
    }

    for col, dtype in dtype_mapping.items():
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(dtype)

    final_columns = [
        'location', 'referenceproduit', 'quantity',
        'total_quantity', 'produituniqueposition',
        'storage_type'
    ]

    return df_clean[[c for c in final_columns if c in df_clean.columns]]
