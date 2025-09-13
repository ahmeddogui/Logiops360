# train_anomaly.py
# ------------------------------------------------------------
# Détection d’anomalies par phase (non supervisé)
# - Données: fv_phase_enriched (phase, carrier, duration_h, stats de ref)
# - Modèle: IsolationForest
# - Sorties: anomaly_iforest.joblib + anomaly_meta.json
# ------------------------------------------------------------
import os, json
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_recall_fscore_support
from joblib import dump

# ========= Config DB =========
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ========= Paramètres modèle =========
SEED = 42
CONTAMINATION = float(os.getenv("ANOM_CONTAM", "0.05"))     # fraction attendue d’outliers
THRESHOLD_QUANTILE = float(os.getenv("ANOM_THRESH_Q", "0.95"))  # seuil posthoc sur score

# ========= Features ==============
CAT_FEATURES = ["phase", "carrier"]
NUM_FEATURES_BASE = ["duration_h","avg_duration_h","p50_duration_h","p90_duration_h","std_duration_h"]

def load_data():
    eng = create_engine(DB_URI)
    with eng.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM fv_phase_enriched"), conn)
    return df

def impute_numeric_base(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    # s'assurer que les colonnes existent
    for c in NUM_FEATURES_BASE:
        if c not in d.columns:
            d[c] = np.nan
    # duration: si NaN -> 0.0 (on filtrera ensuite si besoin)
    d["duration_h"] = d["duration_h"].fillna(0.0)
    # avg/p50/p90: fallback -> duration_h puis 0.0
    for c in ["avg_duration_h","p50_duration_h","p90_duration_h"]:
        d[c] = d[c].fillna(d["duration_h"]).fillna(0.0)
    # std: NaN ou 0 -> 1.0 pour éviter division par 0
    d["std_duration_h"] = d["std_duration_h"].replace(0, np.nan).fillna(1.0)
    return d

def build_features(df: pd.DataFrame):
    df = df.copy()

    # 1) exclure la phase "delivered" (insensible à la casse)
    if "phase" in df.columns:
        before = len(df)
        df = df[df["phase"].str.lower() != "delivered"]
        removed = before - len(df)
        print(f"🧹 Filtre 'delivered' : retiré {removed} lignes")

    # 2) imputation des stats de référence
    df = impute_numeric_base(df)

    # 3) features dérivées robustes
    df["ratio_p50"] = df["duration_h"] / df["p50_duration_h"]
    df["ratio_p90"] = df["duration_h"] / df["p90_duration_h"]
    df["diff_avg"]  = df["duration_h"] - df["avg_duration_h"]
    df["zscore"]    = (df["duration_h"] - df["avg_duration_h"]) / df["std_duration_h"]
    for c in ["ratio_p50","ratio_p90","diff_avg","zscore"]:
        df[c] = df[c].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # 4) ne garder que les lignes avec une durée utile (> 0)
    df = df[df["duration_h"].notnull()].reset_index(drop=True)

    num_feats = NUM_FEATURES_BASE + ["ratio_p50","ratio_p90","diff_avg","zscore"]
    return df, num_feats

def main():
    out_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(out_dir, "anomaly_iforest.joblib")
    meta_path  = os.path.join(out_dir, "anomaly_meta.json")

    print("📥 Chargement des données fv_phase_enriched ...")
    df_raw = load_data()
    print(f"   → {len(df_raw):,} lignes brutes")

    # Construire features
    df, num_feats = build_features(df_raw)
    print(f"   → {len(df):,} lignes après filtrage & features")

    # Encodage catégoriel one-hot (phase, carrier)
    for c in CAT_FEATURES:
        if c not in df.columns:
            df[c] = "UNK"
    df_cat = pd.get_dummies(df[CAT_FEATURES].astype(str), dummy_na=True, prefix=CAT_FEATURES)

    # Numériques + scaling
    X_num  = df[num_feats].astype(float).fillna(0.0)
    num_cols = X_num.columns.tolist()
    oh_cols  = df_cat.columns.tolist()

    scaler = StandardScaler()
    X_num_scaled = scaler.fit_transform(X_num.values)

    # Matrice finale (one-hot + nums scalés)
    X = np.concatenate([df_cat.values.astype(float), X_num_scaled], axis=1)
    # Sécurité ultime
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    # Entraînement IsolationForest
    print("🚀 Entraînement IsolationForest ...")
    iforest = IsolationForest(
        n_estimators=300,
        max_samples="auto",
        contamination=CONTAMINATION,
        random_state=SEED,
        n_jobs=-1
    ).fit(X)

    # Scores (plus négatif => plus anormal). Convertir en [0,1] (1 = plus anormal)
    raw_scores = iforest.score_samples(X)
    ranks = pd.Series(raw_scores).rank(pct=True, ascending=True).values
    anomaly_score = 1.0 - ranks

    # Seuil par quantile global
    thr = float(np.quantile(anomaly_score, THRESHOLD_QUANTILE))
    y_pred = (anomaly_score >= thr).astype(int)

    # Évaluation vs règle (si dispo)
    metrics = {}
    if "is_anomaly_rule" in df.columns and df["is_anomaly_rule"].notnull().any():
        y_true = df["is_anomaly_rule"].fillna(0).astype(int).values
        pr, rc, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary", zero_division=0)
        metrics["weak_labels_eval_vs_rule"] = {"precision": float(pr), "recall": float(rc), "f1": float(f1)}
        metrics["rates"] = {
            "rule_rate": float(df["is_anomaly_rule"].mean()),
            "model_rate": float(y_pred.mean())
        }

    # Sauvegarde artefacts
    dump({
        "scaler_num_cols": num_cols,
        "oh_cols": oh_cols,
        "scaler": scaler,
        "iforest": iforest
    }, model_path)

    meta = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "view_used": "fv_phase_enriched (filtered phase!='delivered')",
        "seed": SEED,
        "contamination": CONTAMINATION,
        "threshold_quantile": THRESHOLD_QUANTILE,
        "decision_threshold": thr,
        "cat_features": CAT_FEATURES,
        "num_features_base": NUM_FEATURES_BASE,
        "num_features_derived": ["ratio_p50","ratio_p90","diff_avg","zscore"],
        "model_path": model_path,
        "metrics": metrics,
        "notes": "Phase 'delivered' exclue au training et dans la vue SQL."
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print("Modèle sauvegardé :", model_path)
    print("Métadonnées       :", meta_path)
    print(f"Seuil (quantile {THRESHOLD_QUANTILE:.2f}) = {thr:.4f}")
    if metrics:
        print("Eval (vs règle P90):", metrics["weak_labels_eval_vs_rule"])

if __name__ == "__main__":
    main()
