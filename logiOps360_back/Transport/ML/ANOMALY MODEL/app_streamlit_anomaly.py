# app_streamlit_anomaly.py
# ------------------------------------------------------------
# Visualisation des anomalies par phase (par shipment)
# - charge: anomaly_iforest.joblib + anomaly_meta.json
# - lit: fv_phase_enriched (sans "delivered" côté SQL, + filtre sécurité ici)
# - sélection d'un shipment -> score anomalies par phase (modèle + règle P90)
# - robustesse: imputation NaN, protections division par zéro
# ------------------------------------------------------------
import os, json
import numpy as np
import pandas as pd
import joblib
import streamlit as st
from sqlalchemy import create_engine, text

# ========= Config DB =========
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ========= Artefacts =========
APP_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(APP_DIR, "anomaly_iforest.joblib")
META_PATH  = os.path.join(APP_DIR, "anomaly_meta.json")

# ========= Features alignées avec train =========
CAT_FEATURES = ["phase", "carrier"]
NUM_BASE = ["duration_h", "avg_duration_h", "p50_duration_h", "p90_duration_h", "std_duration_h"]
NUM_DERIVED = ["ratio_p50","ratio_p90","diff_avg","zscore"]
NUM_ALL = NUM_BASE + NUM_DERIVED

# ================= UI CONFIG =================
st.set_page_config(page_title="LogiOps360 • Anomalies par phase", layout="wide")
st.title("🔎 Détection d’anomalies par phase")
st.caption("Modèle IsolationForest sur `fv_phase_enriched` (+ règle simple P90)")

# ================= Helpers =================
@st.cache_resource
def get_engine():
    return create_engine(DB_URI)

@st.cache_resource
def load_model():
    bundle = joblib.load(MODEL_PATH)
    with open(META_PATH, "r", encoding="utf-8") as f:
        meta = json.load(f)
    scaler = bundle["scaler"]                 # StandardScaler (fit sur colonnes numériques)
    iforest = bundle["iforest"]               # IsolationForest entraîné
    oh_cols_train = bundle["oh_cols"]         # liste des colonnes one-hot vues au train
    num_cols_train = bundle["scaler_num_cols"]# colonnes numériques utilisées au train
    default_thr_q = float(meta.get("threshold_quantile", 0.95))
    return scaler, iforest, oh_cols_train, num_cols_train, meta, default_thr_q

@st.cache_data(ttl=600)
def list_shipments(limit=2000):
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT DISTINCT shipment_id
                FROM fv_phase_enriched
                ORDER BY shipment_id DESC
                LIMIT :lim
            """),
            conn, params={"lim": limit}
        )
    return df["shipment_id"].astype(str).tolist()

@st.cache_data(ttl=300)
def load_phases_for_shipment(shipment_id: str) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM fv_phase_enriched WHERE shipment_id = :sid ORDER BY event_id"),
            conn, params={"sid": shipment_id}
        )
    return df

def impute_numeric_base(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for c in NUM_BASE:
        if c not in d.columns:
            d[c] = np.nan
    d["duration_h"] = d["duration_h"].fillna(0.0)
    for c in ["avg_duration_h","p50_duration_h","p90_duration_h"]:
        d[c] = d[c].fillna(d["duration_h"]).fillna(0.0)
    d["std_duration_h"] = d["std_duration_h"].replace(0, np.nan).fillna(1.0)
    return d

def compute_derived_cols(df: pd.DataFrame) -> pd.DataFrame:
    d = impute_numeric_base(df)
    d["ratio_p50"] = d["duration_h"] / d["p50_duration_h"]
    d["ratio_p90"] = d["duration_h"] / d["p90_duration_h"]
    d["diff_avg"]  = d["duration_h"] - d["avg_duration_h"]
    d["zscore"]    = (d["duration_h"] - d["avg_duration_h"]) / d["std_duration_h"]
    for c in ["ratio_p50","ratio_p90","diff_avg","zscore"]:
        d[c] = d[c].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return d

def build_inference_matrix(df: pd.DataFrame, scaler, oh_cols_train, num_cols_train):
    for c in CAT_FEATURES:
        if c not in df.columns:
            df[c] = "UNK"
    df_cat = pd.get_dummies(df[CAT_FEATURES].astype(str), dummy_na=True, prefix=CAT_FEATURES)

    for col in oh_cols_train:
        if col not in df_cat.columns:
            df_cat[col] = 0.0
    extra_oh = [c for c in df_cat.columns if c not in oh_cols_train]
    if extra_oh:
        df_cat = df_cat.drop(columns=extra_oh, errors="ignore")
    df_cat = df_cat[oh_cols_train]

    for c in num_cols_train:
        if c not in df.columns:
            df[c] = 0.0
    X_num = df[num_cols_train].astype(float).copy().fillna(0.0)
    X_num_scaled = scaler.transform(X_num.values)
    X_num_scaled = pd.DataFrame(X_num_scaled, columns=num_cols_train, index=df.index)

    X = pd.concat([df_cat, X_num_scaled], axis=1).fillna(0.0)
    return X

def score_iforest(iforest, X):
    raw = iforest.score_samples(X)  # plus négatif => plus anormal
    rank_pct = pd.Series(raw).rank(pct=True, ascending=True).values
    anomaly_score = 1.0 - rank_pct
    return raw, anomaly_score

def default_actions_for_phase(phase: str):
    p = str(phase).lower()
    if "custom" in p:  # customs clearance
        return "📞 Contacter broker / fournir docs manquants"
    if "pickup" in p:
        return "📍 Vérifier disponibilité du shipper / replanifier enlèvement"
    if "linehaul" in p or "hub" in p or "transit" in p:
        return "🚚 Escalader transporteur / contrôler statut en temps réel"
    if "last" in p or "delivery" in p:
        return "📦 Notifier client final / proposer créneau alternatif"
    return "🔁 Vérifier exceptions & réessayer / escalade superviseur"

# ================= Load artefacts & UI =================
try:
    scaler, iforest, oh_cols_train, num_cols_train, meta, default_thr_q = load_model()
except Exception as e:
    st.error(f"Impossible de charger les artefacts du modèle : {e}")
    st.stop()

ship_list = list_shipments()
col_top1, col_top2 = st.columns([1.5, 1])
with col_top1:
    chosen = st.selectbox("Shipment", options=ship_list, index=0 if ship_list else None)
with col_top2:
    thr_q = st.slider("Seuil quantile (sensibilité anomalies)", 0.80, 0.999, float(default_thr_q), 0.01,
                      help="Phases au-dessus de ce quantile local sont marquées comme anomalies (plus grand = plus sévère).")

st.divider()

if not ship_list or not chosen:
    st.warning("Aucun shipment disponible.")
    st.stop()

# ================= Inference =================
df = load_phases_for_shipment(chosen)
if df.empty:
    st.warning("Aucune phase trouvée pour ce shipment.")
    st.stop()

# 🚫 Exclure la phase 'delivered' (sécurité supplémentaire)
if "phase" in df.columns:
    before = len(df)
    df = df[df["phase"].str.lower() != "delivered"].copy()
    after = len(df)
    if before != after:
        st.caption(f"Filtre appliqué : {before-after} phase(s) 'delivered' ignorée(s).")

# dérivées + imputations
df = compute_derived_cols(df)

# sécurité: s'assurer que colonnes critiques existent
for c in NUM_ALL + CAT_FEATURES + ["is_anomaly_rule"]:
    if c not in df.columns:
        if c == "is_anomaly_rule":
            df[c] = 0
        else:
            df[c] = 0.0

# Construire matrice d'inférence
X = build_inference_matrix(df, scaler, oh_cols_train, num_cols_train)

# Conversion robuste -> float + imputation finale
try:
    X_arr = np.asarray(X, dtype=float)
except Exception:
    X_arr = pd.DataFrame(X).apply(pd.to_numeric, errors="coerce").to_numpy()
X_arr = np.nan_to_num(X_arr, nan=0.0, posinf=0.0, neginf=0.0)
X = X_arr

# Scorer modèle
try:
    raw, anom_score = score_iforest(iforest, X)
except Exception as e:
    st.error(f"Erreur pendant le scoring du modèle : {e}")
    st.stop()

df["_raw_score"] = raw
df["_anom_score"] = anom_score

# Seuil local par quantile (sur ce shipment)
thr_val = float(np.quantile(df["_anom_score"].values, thr_q)) if len(df) else 1.0
df["_is_anom_model"] = (df["_anom_score"] >= thr_val).astype(int) if len(df) else 0

# ================= Affichage =================
left, right = st.columns([1.2, 1])
with left:
    st.subheader("Phases & scores")
    view_cols = [
        "event_id", "phase", "carrier",
        "duration_h", "p50_duration_h", "p90_duration_h", "zscore",
        "is_anomaly_rule", "_anom_score", "_is_anom_model"
    ]
    present = [c for c in view_cols if c in df.columns]
    df_show = df[present].copy()
    df_show = df_show.rename(columns={
        "duration_h":"Durée (h)",
        "p50_duration_h":"P50 (h)",
        "p90_duration_h":"P90 (h)",
        "zscore":"Z-score",
        "is_anomaly_rule":"Règle>P90",
        "_anom_score":"Score modèle",
        "_is_anom_model":"Anomalie (modèle)"
    })
    if "Score modèle" in df_show.columns:
        df_show["Score modèle"] = (pd.to_numeric(df_show["Score modèle"].str.replace(" %","", regex=False), errors="coerce")/100.0 if df_show["Score modèle"].dtype==object else df_show["Score modèle"])
        df_show["Score modèle"] = (df_show["Score modèle"]*100).round(1).astype(str) + " %"
    if "Règle>P90" in df_show.columns:
        df_show["Règle>P90"] = df_show["Règle>P90"].map({1:"🔴", 0:"🟢", None:"–"})
    if "Anomalie (modèle)" in df_show.columns:
        df_show["Anomalie (modèle)"] = df_show["Anomalie (modèle)"].map({1:"🔴", 0:"🟢"})
    st.dataframe(df_show, use_container_width=True, hide_index=True)

with right:
    n_rule = int(pd.to_numeric(df["is_anomaly_rule"], errors="coerce").fillna(0).sum())
    n_model = int(df["_is_anom_model"].sum())
    st.metric("Anomalies (règle P90)", f"{n_rule}")
    st.metric("Anomalies (modèle)", f"{n_model}")
    st.caption(f"Seuil modèle (quantile local) : q = {thr_q:.3f}, valeur = {thr_val:.3f}")

st.divider()

# Détails + actions
st.subheader("Actions suggérées")
alert_rows = df[df["_is_anom_model"]==1].copy()
if alert_rows.empty:
    st.success("✅ Aucune anomalie détectée par le modèle pour ce shipment.")
else:
    for _, r in alert_rows.iterrows():
        action = default_actions_for_phase(r.get("phase", ""))
        try:
            txt = f"**Phase**: {r.get('phase','?')} • **Durée**: {float(r.get('duration_h',0)):.1f} h • **Score**: {float(r.get('_anom_score',0)):.2f} → {action}"
        except Exception:
            txt = f"**Phase**: {r.get('phase','?')} • **Score**: {r.get('_anom_score','?')} → {action}"
        st.write(txt)

with st.expander("Données brutes"):
    st.write(df.drop(columns=["_raw_score","_anom_score","_is_anom_model"], errors="ignore"))
