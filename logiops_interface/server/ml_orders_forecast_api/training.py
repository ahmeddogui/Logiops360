# server/ml_orders_forecast_api/training.py
from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# --- chemins de sortie
BASE_DIR   = Path(__file__).resolve().parents[1]      # .../server
OUT_DIR    = BASE_DIR / "models" / "orders_forecast"
DAILY_CSV  = OUT_DIR / "predictions_daily.csv"
WEEKLY_CSV = OUT_DIR / "predictions_weekly.csv"   # facultatif (on peut laisser vide)
MONTH_CSV  = OUT_DIR / "predictions_monthly.csv"  # facultatif (on peut laisser vide)

OUT_DIR.mkdir(parents=True, exist_ok=True)

def _get_engine():
    # mêmes variables d’env que le backend
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASS = os.getenv("DB_PASS", "kdh")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "logiops")

    url = URL.create(
        "postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    return create_engine(url, future=True, pool_pre_ping=True)

def _read_orders() -> pd.DataFrame:
    """Lit clean_customer_orders(creationdate) et renvoie un DF jour/qty_real trié."""
    eng = _get_engine()
    with eng.connect() as cn:
        # crée la table si absente (sécurité)
        cn.execute(text("""
            CREATE TABLE IF NOT EXISTS clean_customer_orders (
                creationdate TIMESTAMP WITHOUT TIME ZONE
            )
        """))
        df = pd.read_sql(
            text("""
                SELECT DATE(creationdate) AS day
                FROM clean_customer_orders
                WHERE creationdate IS NOT NULL
            """),
            cn,
        )
    if df.empty:
        return pd.DataFrame(columns=["day", "qty_real"])
    s = df["day"].value_counts().rename_axis("day").sort_index()
    out = s.rename("qty_real").reset_index()
    out["day"] = pd.to_datetime(out["day"]).dt.date
    return out

def _build_daily_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construit predictions_daily.csv au format:
      day, qty_real, qty_pred
    Logique simple: qty_pred = moyenne mobile 7 jours (min 3), puis
    ajoute 'demain' avec la dernière moyenne.
    """
    if df.empty:
        today = datetime.now().date()
        return pd.DataFrame([{"day": today, "qty_real": 0, "qty_pred": 0}])

    ser = df.set_index("day")["qty_real"].sort_index()
    roll = ser.rolling(7, min_periods=3).mean()
    base_pred = roll.fillna(ser.expanding().mean())

    # ajoute demain pour s'assurer qu'on a une prévision future
    tomorrow = (ser.index.max() + timedelta(days=1))
    tomorrow_pred = float(base_pred.iloc[-1])
    fut = pd.Series([tomorrow_pred], index=[tomorrow], name="qty_pred")

    pred = pd.concat([base_pred.rename("qty_pred"), fut])
    pred = pred.reset_index().rename(columns={"index": "day"})

    # merge avec qty_real (NaN pour le futur)
    out = pd.merge(
        left=pred,
        right=ser.reset_index().rename(columns={"qty_real": "qty_real"}),
        how="left",
        on="day",
    )
    # ordre colonnes + types
    out = out[["day", "qty_real", "qty_pred"]].copy()
    out["qty_real"] = pd.to_numeric(out["qty_real"], errors="coerce")
    out["qty_pred"] = pd.to_numeric(out["qty_pred"], errors="coerce")
    # cast dates YYYY-MM-DD
    out["day"] = pd.to_datetime(out["day"]).dt.date
    return out

def run() -> str:
    df = _read_orders()
    daily = _build_daily_forecast(df)

    # écrit uniquement le daily; weekly/monthly seront dérivés côté API si absents
    daily.sort_values("day").to_csv(DAILY_CSV, index=False)

    return f"wrote {len(daily)} rows to {DAILY_CSV.name}"

# alias fréquents pour compat compat avec d’autres imports
def train() -> str:  return run()
def retrain() -> str:return run()
def main() -> None:
    msg = run()
    print(msg)

if __name__ == "__main__":
    main()
