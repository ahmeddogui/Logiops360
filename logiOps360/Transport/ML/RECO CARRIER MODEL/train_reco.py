import os, json
from datetime import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error
import lightgbm as lgb
import joblib

# ---- Connexion DB ----
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ---- Features ----
CATEGORICAL = ["origin","destination_zone","carrier","service_level","ship_dow","ship_hour"]
NUMERIC = [
    "distance_km","weight_kg","volume_m3","total_units","n_lines",
    "p50_eta_h","p90_eta_h","delay_rate",
    "cp_cost_baseline_eur","on_time_rate","capacity_score"
]
FEATURES = CATEGORICAL + NUMERIC

SEED=42
LGB_PARAMS = dict(
    n_estimators=800,
    learning_rate=0.03,
    num_leaves=64,
    subsample=0.9,
    colsample_bytree=0.8,
    random_state=SEED
)

def load_df():
    eng = create_engine(DB_URI)
    with eng.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM fv_train_carrier_choice"), conn)
    df["ship_day"] = pd.to_datetime(df["ship_day"])
    return df

def split_time(df, q1=0.7, q2=0.85):
    df = df.sort_values("ship_day")
    cut1, cut2 = df["ship_day"].quantile(q1), df["ship_day"].quantile(q2)
    tr = df[df.ship_day <= cut1]
    va = df[(df.ship_day > cut1) & (df.ship_day <= cut2)]
    te = df[df.ship_day > cut2]
    return tr, va, te

def build_pipe():
    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
        ("num", "passthrough", NUMERIC)
    ])
    return Pipeline([("pre", pre), ("model", lgb.LGBMRegressor(**LGB_PARAMS))])

def eval_reg(y, yhat, tag):
    mae = mean_absolute_error(y, yhat)
    rmse = mean_squared_error(y, yhat, squared=False)
    print(f"{tag} MAE={mae:.3f} | RMSE={rmse:.3f}")
    return dict(mae=float(mae), rmse=float(rmse))

def main():
    out_dir = os.path.dirname(os.path.abspath(__file__))
    df = load_df()
    tr, va, te = split_time(df)
    print(f"📦 train={len(tr)} valid={len(va)} test={len(te)}")

    # ===== ETA per carrier =====
    Xtr, ytr = tr[FEATURES], tr["eta_h"]
    Xva, yva = va[FEATURES], va["eta_h"]
    Xte, yte = te[FEATURES], te["eta_h"]

    pipe_eta = build_pipe()
    print("🚀 Train ETA regressor …")
    pipe_eta.fit(Xtr, ytr)

    m_va_eta = eval_reg(yva, pipe_eta.predict(Xva), "[ETA][VAL]")
    m_te_eta = eval_reg(yte, pipe_eta.predict(Xte), "[ETA][TST]")

    joblib.dump(pipe_eta, os.path.join(out_dir, "eta_carrier_lgbm.joblib"))

    # ===== Cost per carrier (optional) =====
    has_cost = df["actual_total_cost_eur"].notnull().sum() > 0
    metrics_cost = None
    if has_cost:
        Xtr, ytr = tr[FEATURES], tr["actual_total_cost_eur"]
        Xva, yva = va[FEATURES], va["actual_total_cost_eur"]
        Xte, yte = te[FEATURES], te["actual_total_cost_eur"]

        pipe_cost = build_pipe()
        print("💶 Train COST regressor …")
        pipe_cost.fit(Xtr, ytr)

        m_va_c = eval_reg(yva, pipe_cost.predict(Xva), "[COST][VAL]")
        m_te_c = eval_reg(yte, pipe_cost.predict(Xte), "[COST][TST]")

        joblib.dump(pipe_cost, os.path.join(out_dir, "cost_lgbm.joblib"))
        metrics_cost = {"valid": m_va_c, "test": m_te_c}
    else:
        print("ℹ️ Pas de colonne coût réel → utilisation proxy cp_cost_baseline_eur en prod.")

    # Sauvegarde meta JSON
    meta = {
        "generated_at": datetime.utcnow().isoformat()+"Z",
        "features": FEATURES,
        "categorical": CATEGORICAL,
        "numeric": NUMERIC,
        "eta_model_path": os.path.join(out_dir, "eta_carrier_lgbm.joblib"),
        "cost_model_path": os.path.join(out_dir, "cost_lgbm.joblib") if has_cost else None,
        "metrics": {
            "eta": {"valid": m_va_eta, "test": m_te_eta},
            "cost": metrics_cost
        },
        "params": LGB_PARAMS,
        "notes": "Ranking = pondération min-max sur cost_pred, eta_pred et risk (1-on_time_rate)."
    }
    with open(os.path.join(out_dir, "reco_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print("✅ Artefacts sauvegardés (eta_carrier_lgbm.joblib, cost_lgbm.joblib?, reco_meta.json)")

if __name__ == "__main__":
    main()
