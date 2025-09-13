# train_eta.py
# ------------------------------------------------------------
# Entraîne un modèle ETA (régression) depuis la vue fv_train_eta
# Sorties:
#   - models/eta_lgbm.joblib          (pipeline préprocessing + modèle)
#   - models/eta_feature_meta.json    (liste des features & colonnes cat/num)
#   - impression des métriques MAE/RMSE sur validation et test
# ------------------------------------------------------------

import os
import json
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error

import lightgbm as lgb
import joblib


# ============
# Config DB
# ============
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")

DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ============
# Hyperparams
# ============
SEED = 42
N_ESTIMATORS = 1200
LEARNING_RATE = 0.03
NUM_LEAVES = 64
SUBSAMPLE = 0.9
COLSAMPLE_BYTREE = 0.8

# ============
# Features (doivent matcher la vue fv_train_eta)
# ============
CATEGORICAL = [
    "origin",
    "destination_zone",
    "carrier",
    "service_level",
    "ship_dow",
    "ship_hour",
]
NUMERIC = [
    "distance_km",
    "weight_kg",
    "volume_m3",
    "total_units",
    "n_lines",
    # "sla_hours",  # tu peux l'activer si tu veux l'inclure comme feature
]
FEATURES = CATEGORICAL + NUMERIC
TARGET = "target_eta_hours"
DATE_COL = "ship_day"       # pour split temporel (présent dans la vue)
SHIP_DT_COL = "ship_dt"     # pour debug éventuel


def load_data():
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM fv_train_eta"), conn)

    # Supprimer les lignes sans target
    df = df.dropna(subset=[TARGET])

    # 🔥 Correction timezone : cast ship_day en datetime naïf
    if not np.issubdtype(df[DATE_COL].dtype, np.datetime64):
        tmp = pd.to_datetime(df[DATE_COL], utc=True)   # tz-aware UTC
        df[DATE_COL] = tmp.dt.tz_localize(None)        # tz-naïf

    return df



def time_split(df: pd.DataFrame, q_train=0.70, q_valid=0.85):
    """Split temporel: train jusqu’au quantile q_train, valid entre q_train..q_valid, test après q_valid."""
    df = df.sort_values(DATE_COL).reset_index(drop=True)
    cut1 = df[DATE_COL].quantile(q_train)
    cut2 = df[DATE_COL].quantile(q_valid)

    train = df[df[DATE_COL] <= cut1]
    valid = df[(df[DATE_COL] > cut1) & (df[DATE_COL] <= cut2)]
    test  = df[df[DATE_COL] > cut2]

    return train, valid, test


def build_pipeline():
    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
            ("num", "passthrough", NUMERIC)
        ]
    )
    model = lgb.LGBMRegressor(
        n_estimators=N_ESTIMATORS,
        learning_rate=LEARNING_RATE,
        num_leaves=NUM_LEAVES,
        subsample=SUBSAMPLE,
        colsample_bytree=COLSAMPLE_BYTREE,
        random_state=SEED
    )
    pipe = Pipeline(steps=[("pre", pre), ("model", model)])
    return pipe


def evaluate(y_true, y_pred, prefix=""):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    print(f"{prefix}MAE (h):  {mae:.3f}")
    print(f"{prefix}RMSE (h): {rmse:.3f}")
    return {"mae": float(mae), "rmse": float(rmse)}


def save_artifacts(pipeline, metrics_valid, metrics_test):
    # On récupère le dossier courant du script
    out_dir = os.path.dirname(os.path.abspath(__file__))

    model_path = os.path.join(out_dir, "eta_lgbm.joblib")
    meta_path = os.path.join(out_dir, "eta_feature_meta.json")

    joblib.dump(pipeline, model_path)

    meta = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "model_path": model_path,
        "features": FEATURES,
        "categorical": CATEGORICAL,
        "numeric": NUMERIC,
        "target": TARGET,
        "metrics_valid": metrics_valid,
        "metrics_test": metrics_test,
        "algo": "LightGBMRegressor",
        "params": {
            "n_estimators": N_ESTIMATORS,
            "learning_rate": LEARNING_RATE,
            "num_leaves": NUM_LEAVES,
            "subsample": SUBSAMPLE,
            "colsample_bytree": COLSAMPLE_BYTREE,
            "random_state": SEED
        }
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"Modèle sauvegardé: {model_path}")
    print(f"Métadonnées: {meta_path}")


def main():
    print("Chargement des données depuis fv_train_eta ...")
    df = load_data()
    print(f"   → {len(df):,} lignes")

    # Vérifier la présence des features
    missing = [c for c in FEATURES + [TARGET, DATE_COL] if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans fv_train_eta: {missing}")

    # Split temporel
    train, valid, test = time_split(df)
    print(f"Split temporel: train={len(train)}, valid={len(valid)}, test={len(test)}")

    X_tr, y_tr = train[FEATURES], train[TARGET]
    X_va, y_va = valid[FEATURES], valid[TARGET]
    X_te, y_te = test[FEATURES],  test[TARGET]

    # Pipeline
    pipe = build_pipeline()

    print("Entraînement LightGBM ...")
    pipe.fit(X_tr, y_tr)

    # Évaluation
    print("\nÉvaluation:")
    pred_va = pipe.predict(X_va)
    m_valid = evaluate(y_va, pred_va, prefix="[VALID] ")

    pred_te = pipe.predict(X_te)
    m_test = evaluate(y_te, pred_te, prefix="[TEST ] ")

    # Sauvegarde
    save_artifacts(pipe, m_valid, m_test)

    # Astuce: aperçu des importances (globales) via feature_names_out
    try:
        pre = pipe.named_steps["pre"]
        model = pipe.named_steps["model"]
        # noms des colonnes après OneHotEncoder
        feat_names = pre.get_feature_names_out()
        importances = getattr(model, "feature_importances_", None)
        if importances is not None:
            imp = (
                pd.DataFrame({"feature": feat_names, "importance": importances})
                .sort_values("importance", ascending=False)
                .head(20)
            )
            print("\n Top 20 features (global importance):")
            print(imp.to_string(index=False))
    except Exception as e:
        print(f"(info) Impossible d’afficher les importances: {e}")


if __name__ == "__main__":
    main()
