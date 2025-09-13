import os
import sys
import importlib
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.db_utils import connect_db

TRANSFORM_FUNCS = [
    "Transport.Transformations.transform_monthly_modal.transform_monthly_modal",
    "Transport.Transformations.transform_smart_logistics_dataset.transform_smart_logistics_dataset",
    "Transport.Transformations.transform_supply_chain_problem.transform_supply_chain_problem",
    "Transport.Transformations.transform_transportation_and_logistics.transform_transportation_and_logistics"
]

TABLE_NAME_OVERRIDES = {
    "transform_supply_chain_problem": "clean_supply_chain_logistic_problem_transport"
}

def resolve_callable(dotted_path: str):
    module_path, func_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)

def main():
    print(">>> MAIN TRANSPORT LANCÉ")
    engine = connect_db()
    for dotted_path in TRANSFORM_FUNCS:
        transform_fn = resolve_callable(dotted_path)
        fn_name = transform_fn.__name__
        table_name = TABLE_NAME_OVERRIDES.get(fn_name, f"clean_{fn_name.replace('transform_', '')}")
        try:
            df = transform_fn(engine)
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"{table_name} : {len(df)} lignes insérées")
        except Exception as e:
            print(f"{table_name} : erreur - {e}")
    print("Transformations TRANSPORT terminées.")

if __name__ == "__main__":
    main()
