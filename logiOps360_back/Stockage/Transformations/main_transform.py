import os
import sys
import importlib
from pathlib import Path
import pandas as pd
from sqlalchemy import text, inspect


project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.db_utils import connect_db

TRANSFORM_FUNCS = [
    "Stockage.Transformations.transform_class_based_storage.transform_class_based_storage",
    "Stockage.Transformations.transform_dedicated_storage.transform_dedicated_storage",
    "Stockage.Transformations.transform_support_points.transform_support_points",
    "Stockage.Transformations.transform_random_storage.transform_random_storage",
    "Stockage.Transformations.transform_hybrid_storage.transform_hybrid_storage",
    "Stockage.Transformations.transform_storage_location.transform_storage_location"
]

def resolve_callable(dotted_path: str):
    module_path, func_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)

def split_schema(table_name, default_schema):
    if "." in table_name:
        s, t = table_name.split(".", 1)
        return s.strip('"'), t.strip('"')
    return default_schema, table_name

def safe_overwrite(engine, df: pd.DataFrame, full_table_name: str, default_schema="public"):
    schema, table = split_schema(full_table_name, default_schema)
    insp = inspect(engine)
    with engine.begin() as con:
        exists = insp.has_table(table, schema=schema)
        if exists:
            cols = [c["name"] for c in insp.get_columns(table, schema=schema)]
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            con.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))
            df.to_sql(table, con, schema=schema, if_exists="append", index=False)
        else:
            df.to_sql(table, con, schema=schema, if_exists="replace", index=False)

def create_unified_storage_view(engine):
    view_sql = """
    CREATE OR REPLACE VIEW unified_storage_view AS
    WITH all_storage AS (
        SELECT location, class, referenceproduit, quantity, storage_type FROM clean_class_based_storage
        UNION ALL
        SELECT location, class, referenceproduit, quantity, storage_type FROM clean_dedicated_storage
        UNION ALL
        SELECT location, NULL AS class, referenceproduit, quantity, storage_type FROM clean_random_storage
    ),
    aggregated AS (
        SELECT location, referenceproduit, storage_type, SUM(quantity) AS total_quantity
        FROM all_storage
        GROUP BY location, referenceproduit, storage_type
    )
    SELECT
        location,
        referenceproduit,
        COALESCE(MAX(CASE WHEN storage_type = 'class_based' THEN total_quantity END), 0) AS qty_class_based,
        COALESCE(MAX(CASE WHEN storage_type = 'dedicated' THEN total_quantity END), 0) AS qty_dedicated,
        COALESCE(MAX(CASE WHEN storage_type = 'random' THEN total_quantity END), 0) AS qty_random
    FROM aggregated
    GROUP BY location, referenceproduit
    ORDER BY location, referenceproduit;
    """
    with engine.begin() as con:
        con.execute(text(view_sql))
    print("Vue unified_storage_view créée avec succès")

def main():
    print(">>> MAIN LANCÉ")
    engine = connect_db()
    schema_default = os.getenv("PG_SCHEMA", "public")
    for dotted_path in TRANSFORM_FUNCS:
        transform_fn = resolve_callable(dotted_path)
        print(f"Execution de la fonction : {transform_fn.__name__}")
        table_suffix = transform_fn.__name__.replace("transform_", "")
        table_name = f"clean_{table_suffix}"
        try:
            df = transform_fn(engine)
            safe_overwrite(engine, df, table_name, default_schema=schema_default)
            print(f"{table_name} : {len(df)} lignes insérées")
        except Exception as e:
            print(f"{table_name} : erreur - {e}")
    try:
        create_unified_storage_view(engine)
    except Exception as e:
        print(f"Erreur lors de la création de la vue unified_storage_view : {e}")
    print("Transformations terminées.")

if __name__ == "__main__":
    main()
