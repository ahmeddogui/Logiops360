import os, sys
from pathlib import Path


project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.db_utils import connect_db

try:
    from .forecasts import DemandForecaster
except Exception:
    try:
        from Commandes.Models.forecasts import DemandForecaster
    except Exception:
        from forecasts import DemandForecaster

def main():
    eng = connect_db()
    forecaster = DemandForecaster(engine=eng, horizon_weeks=8, min_history_weeks=8, top_refs=None, table_raw="clean_customer_orders")
    fc = forecaster.run()
    print(fc.head())

if __name__ == "__main__":
    main()
