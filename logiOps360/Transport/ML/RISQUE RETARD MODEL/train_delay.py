# train_delay.py
# ------------------------------------------------------------
# Entraîne un modèle classification (retard binaire) depuis fv_train_delay
# Sorties:
#   - delay_lgbm.joblib
#   - delay_feature_meta.json
# ------------------------------------------------------------
import os, json
from datetime import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    precision_recall_curve, f1_score, confusion_matrix
)
import lightgbm as lgb
import joblib

# ---- DB ----
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ---- Features / cible ----
CATEGORICAL = ["origin", "destination_zone", "carrier", "service_level", "ship_dow", "ship_hour"]
NUMERIC     = ["distance_km", "weight_kg", "volume_m3", "total_units", "n_lines"]
FEATURES = CATEGORICAL + NUMERIC
TARGET   = "is_late"
DATE_COL = "ship_day"

SEED = 42

def load_data():
    eng = create_engine(DB_URI)
    with eng.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM fv_train_delay"), conn)

    # Nettoyage
    df = df.dropna(subset=[TARGET])
    # Normalise ship_day -> datetime naïf
    if not np.issubdtype(df[DATE_COL].dtype, np.datetime64):
        tmp = pd.to_datetime(df[DATE_COL], utc=True, errors="coerce")
        # si c'est un "date" côté SQL, pas besoin d'utc=True; garde cette ligne pour robustesse :
        df[DATE_COL] = tmp.dt.tz_localize(None) if hasattr(tmp.dt, "tz_localize") else pd.to_datetime(df[DATE_COL])

    return df

def time_split(df, q_train=0.70, q_valid=0.85):
    df = df.sort_values(DATE_COL).reset_index(drop=True)
    cut1 = df[DATE_COL].quantile(q_train)
    cut2 = df[DATE_COL].quantile(q_valid)
    train = df[df[DATE_COL] <= cut1]
    valid = df[(df[DATE_COL] > cut1) & (df[DATE_COL] <= cut2)]
    test  = df[df[DATE_COL] > cut2]
    return train, valid, test

def build_pipeline():
    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
        ("num", "passthrough", NUMERIC)
    ])
    clf = lgb.LGBMClassifier(
        n_estimators=1200,
        learning_rate=0.03,
        num_leaves=64,
        subsample=0.9,
        colsample_bytree=0.8,
        class_weight="balanced",  # important si dataset déséquilibré
        random_state=SEED
    )
    return Pipeline([("pre", pre), ("model", clf)])

def eval_threshold(y_true, y_proba):
    # Choix de seuil data-driven via PR-curve (Youden-like sur PR) -> maximise F1
    ps, rs, ts = precision_recall_curve(y_true, y_proba)
    f1s = 2 * ps * rs / (ps + rs + 1e-9)
    idx = int(np.nanargmax(f1s))
    best_thr = ts[idx-1] if idx > 0 and idx-1 < len(ts) else 0.5
    f1 = f1s[idx]
    return float(best_thr), float(f1)

def main():
    print("Chargement fv_train_delay ...")
    df = load_data()
    print(f"   → {len(df):,} lignes")

    missing = [c for c in FEATURES + [TARGET, DATE_COL] if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes: {missing}")

    y_rate = df[TARGET].mean()
    print(f"   → Taux de retard (global): {100*y_rate:.2f}%")

    train, valid, test = time_split(df)
    print(f" Split: train={len(train)}, valid={len(valid)}, test={len(test)}")

    X_tr, y_tr = train[FEATURES], train[TARGET].astype(int)
    X_va, y_va = valid[FEATURES], valid[TARGET].astype(int)
    X_te, y_te = test[FEATURES],  test[TARGET].astype(int)

    pipe = build_pipeline()
    print("Entraînement LightGBMClassifier ...")
    pipe.fit(X_tr, y_tr)

    # Probabilités
    p_va = pipe.predict_proba(X_va)[:,1]
    p_te = pipe.predict_proba(X_te)[:,1]

    # Metrics
    auc_va = roc_auc_score(y_va, p_va)
    ap_va  = average_precision_score(y_va, p_va)

    auc_te = roc_auc_score(y_te, p_te)
    ap_te  = average_precision_score(y_te, p_te)

    thr, f1_va = eval_threshold(y_va, p_va)
    y_pred_te = (p_te >= thr).astype(int)
    f1_te = f1_score(y_te, y_pred_te)
    cm_te = confusion_matrix(y_te, y_pred_te).tolist()

    print("\nValidation:")
    print(f"  AUC: {auc_va:.3f} | AP (PR-AUC): {ap_va:.3f} | Best thr: {thr:.3f} | F1@thr: {f1_va:.3f}")
    print("Test:")
    print(f"  AUC: {auc_te:.3f} | AP (PR-AUC): {ap_te:.3f} | F1@thr: {f1_te:.3f}")
    print(f"  Confusion matrix @thr={thr:.3f} (test): {cm_te}  # [[TN, FP],[FN, TP]]")

    # Sauvegarde artefacts dans le même dossier que ce script
    out_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(out_dir, "delay_lgbm.joblib")
    meta_path  = os.path.join(out_dir, "delay_feature_meta.json")
    joblib.dump(pipe, model_path)

    meta = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "model_path": model_path,
        "features": FEATURES,
        "categorical": CATEGORICAL,
        "numeric": NUMERIC,
        "target": TARGET,
        "metrics": {
            "valid": {"auc": float(auc_va), "ap": float(ap_va), "best_threshold": thr, "f1_at_thr": float(f1_va)},
            "test":  {"auc": float(auc_te), "ap": float(ap_te), "f1_at_thr": float(f1_te), "confusion_matrix": cm_te}
        },
        "algo": "LightGBMClassifier",
        "params": {
            "n_estimators": 1200, "learning_rate": 0.03, "num_leaves": 64,
            "subsample": 0.9, "colsample_bytree": 0.8, "class_weight": "balanced",
            "random_state": SEED
        },
        "decision_threshold": thr
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"\nModèle sauvegardé: {model_path}")
    print(f"Métadonnées: {meta_path}")
    print("Utilise le seuil stocké dans le JSON pour la prod.")
    
if __name__ == "__main__":
    main()
