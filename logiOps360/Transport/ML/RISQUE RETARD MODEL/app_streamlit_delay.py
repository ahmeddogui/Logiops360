# app_streamlit_delay.py
# ------------------------------------------------------------
# Demo Streamlit : Modèle Risque de Retard
# - charge: delay_lgbm.joblib + delay_feature_meta.json
# - lit la vue: fv_train_delay
# - par Shipment ID -> probabilité de retard + verdict (selon seuil)
# ------------------------------------------------------------
import os, json
import pandas as pd
import joblib
import streamlit as st
from sqlalchemy import create_engine, text

# ---- Config DB ----
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# ---- Artefacts ----
APP_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(APP_DIR, "delay_lgbm.joblib")
META_PATH  = os.path.join(APP_DIR, "delay_feature_meta.json")

# ---- Features (doivent matcher l'entraînement) ----
FEATURES = [
    "origin", "destination_zone", "carrier", "service_level",
    "ship_dow", "ship_hour",
    "distance_km", "weight_kg", "volume_m3", "total_units", "n_lines",
]
TARGET = "is_late"
SLA_COL = "sla_hours"
SHIP_DT = "ship_dt"

# ================================
# Streamlit UI
# ================================
st.set_page_config(page_title="LogiOps360 • Risque de retard", layout="wide")
st.title("⏱️ Risque de retard (classification)")
st.caption("Modèle LightGBM entraîné sur `fv_train_delay`")

# ---- Helpers ----
@st.cache_resource
def get_engine():
    return create_engine(DB_URI)

@st.cache_resource
def load_model_and_meta():
    model = joblib.load(MODEL_PATH)
    with open(META_PATH, "r", encoding="utf-8") as f:
        meta = json.load(f)
    thr = float(meta.get("decision_threshold", 0.5))
    return model, meta, thr

@st.cache_data(ttl=300)
def load_ids(limit=2000):
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(
            text("SELECT shipment_id, ship_dt FROM fv_train_delay ORDER BY ship_dt DESC LIMIT :lim"),
            conn, params={"lim": limit}
        )
    df["ship_dt"] = pd.to_datetime(df["ship_dt"], utc=True, errors="coerce")
    df["ship_dt"] = df["ship_dt"].dt.tz_convert("Europe/Paris").dt.tz_localize(None)
    return df

@st.cache_data(ttl=300)
def get_row(shipment_id: str) -> pd.Series:
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM fv_train_delay WHERE shipment_id = :sid"),
                         conn, params={"sid": shipment_id})
    if df.empty:
        raise ValueError("Shipment introuvable.")
    df[SHIP_DT] = pd.to_datetime(df[SHIP_DT], utc=True, errors="coerce").dt.tz_convert("Europe/Paris").dt.tz_localize(None)
    return df.iloc[0]

# ---- Load model + seuil ----
model, meta, THR = load_model_and_meta()

# ---- UI selection ----
colL, colR = st.columns([1.2, 1])
with colL:
    ids = load_ids()
    if ids.empty:
        st.error("fv_train_delay est vide.")
        st.stop()
    chosen = st.selectbox("Shipment ID", ids["shipment_id"].astype(str).tolist(), index=0)
with colR:
    st.info(f"Seuil décision: **{THR:.2f}** (optimisé F1 sur validation)")

st.divider()

# ---- Prédiction ----
row = get_row(chosen)
X = row[FEATURES].to_frame().T
proba = float(model.predict_proba(X)[0,1])

k1, k2, k3 = st.columns(3)
k1.metric("Probabilité de retard", f"{proba*100:.1f}%")
if pd.notnull(row.get(SLA_COL, None)) and pd.notnull(row.get("target_eta_hours", None)):
    delta = float(row["target_eta_hours"]) - float(row[SLA_COL])
    k2.metric("Delta réel (historique)", f"{delta:+.2f} h")
k3.metric("Date d'expédition", row[SHIP_DT].strftime("%Y-%m-%d %H:%M"))

# Verdict selon seuil
if proba >= THR:
    st.error("⚠️ À risque de retard (au-dessus du seuil)")
else:
    st.success("✅ Faible risque (en-dessous du seuil)")

with st.expander("Détails expédition"):
    st.write(row[FEATURES + [SLA_COL, "target_eta_hours"]])

st.caption(f"Modèle: delay_lgbm.joblib • Vue: fv_train_delay • Seuil: {THR:.2f}")
