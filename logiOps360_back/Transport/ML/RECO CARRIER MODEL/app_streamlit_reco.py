import os, json
import numpy as np
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
ETA_MODEL_PATH  = os.path.join(APP_DIR, "eta_carrier_lgbm.joblib")
COST_MODEL_PATH = os.path.join(APP_DIR, "cost_lgbm.joblib")  # peut ne pas exister
META_PATH       = os.path.join(APP_DIR, "reco_meta.json")

# ---- Features alignées avec entraînement ----
FEATURES = [
    "origin","destination_zone","carrier","service_level","ship_dow","ship_hour",
    "distance_km","weight_kg","volume_m3","total_units","n_lines",
    "p50_eta_h","p90_eta_h","delay_rate",
    "cp_cost_baseline_eur","on_time_rate","capacity_score"
]

st.set_page_config(page_title="LogiOps360 • Recommandation transporteur", layout="wide")
st.title("🚚 Recommandation transporteur / mode optimal")
st.caption("Score multicritère basé sur ETA, coût, fiabilité")

# --- Helpers ---
@st.cache_resource
def get_engine():
    return create_engine(DB_URI)

@st.cache_resource
def load_models():
    eta = joblib.load(ETA_MODEL_PATH)
    cost = None
    if os.path.exists(COST_MODEL_PATH):
        try:
            cost = joblib.load(COST_MODEL_PATH)
        except Exception:
            cost = None
    with open(META_PATH, "r", encoding="utf-8") as f:
        meta = json.load(f)
    return eta, cost, meta

@st.cache_data(ttl=600)
def load_form_options():
    eng = get_engine()
    with eng.connect() as conn:
        lanes = pd.read_sql(text("SELECT DISTINCT origin, destination_zone FROM shipments"), conn)
        svcs  = pd.read_sql(text("SELECT DISTINCT service_level FROM carrier_profiles"), conn)
    return lanes, svcs

@st.cache_data(ttl=300)
def get_candidates(origin, destination_zone, service_level, distance, weight):
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
              :origin AS origin, :dest AS destination_zone,
              cp.carrier, :svc AS service_level,
              (1.0 - COALESCE(cp.exception_rate, 0.15))::double precision AS on_time_rate,
              (COALESCE(cp.base_rate_per_km,0) * :dist
               + COALESCE(cp.surcharge_per_kg,0) * :wt)::double precision AS cp_cost_baseline_eur,
              1.0::double precision AS capacity_score,
              lcs.p50_eta_h, lcs.p90_eta_h, lcs.delay_rate
            FROM carrier_profiles cp
            LEFT JOIN fv_lane_carrier_stats lcs
              ON lcs.carrier=cp.carrier
             AND lcs.service_level=cp.service_level
             AND lcs.origin=:origin AND lcs.destination_zone=:dest
            WHERE cp.service_level = :svc
        """), conn, params={"origin": origin, "dest": destination_zone, "svc": service_level,
                            "dist": distance, "wt": weight})
    return df

# --- Load models ---
eta_model, cost_model, meta = load_models()
lanes, svcs = load_form_options()

# --- UI form ---
c1, c2, c3 = st.columns(3)
origin = c1.selectbox("Origin", sorted(lanes["origin"].unique().tolist()))
dest   = c2.selectbox("Destination zone", sorted(lanes["destination_zone"].unique().tolist()))
svc    = c3.selectbox("Service level", sorted(svcs["service_level"].dropna().unique().tolist()))

c4, c5, c6 = st.columns(3)
distance = c4.number_input("Distance (km)", min_value=0.0, value=500.0, step=10.0)
weight   = c5.number_input("Weight (kg)",   min_value=0.0, value=120.0, step=5.0)
volume   = c6.number_input("Volume (m3)",   min_value=0.0, value=1.2,  step=0.1)
units    = c4.number_input("Total units",   min_value=0,   value=10,   step=1)
n_lines  = c5.number_input("N lines",       min_value=0,   value=3,    step=1)
ship_dow = c6.number_input("Ship DOW (0=dim,6=sam)", min_value=0, max_value=6, value=2)
ship_hr  = c4.number_input("Ship hour",     min_value=0, max_value=23, value=10)

w_cost = c5.slider("Poids coût", 0.0, 1.0, 0.5, 0.05)
w_eta  = c6.slider("Poids délai",0.0, 1.0, 0.3, 0.05)
w_risk = c4.slider("Poids risque",0.0, 1.0, 0.2, 0.05)

# --- Action ---
if st.button("Calculer le ranking"):
    cands = get_candidates(origin, dest, svc, distance, weight)
    if cands.empty:
        st.warning("Aucun transporteur candidat pour cette lane/service.")
        st.stop()

    # compléter les features
    cands = cands.assign(
        ship_dow=int(ship_dow), ship_hour=int(ship_hr),
        distance_km=float(distance), weight_kg=float(weight), volume_m3=float(volume),
        total_units=int(units), n_lines=int(n_lines)
    )

    # ETA prédite
    X = cands[FEATURES].copy()
    eta_pred = eta_model.predict(X)
    cands["eta_pred_h"] = eta_pred

    # Coût prédit ou proxy
    if cost_model is not None:
        try:
            cost_pred = cost_model.predict(X)
        except Exception as e:
            st.warning(f"Erreur modèle coût, fallback cp_cost_baseline_eur: {e}")
            cost_pred = cands["cp_cost_baseline_eur"].to_numpy()
    else:
        cost_pred = cands["cp_cost_baseline_eur"].to_numpy()
    cands["cost_pred"] = cost_pred

    # Risque (proxy)
    cands["risk"] = 1 - cands["on_time_rate"]

    # Normalisations min-max
    def mm(x):
        x = np.asarray(x, dtype=float)
        rng = np.max(x) - np.min(x)
        return (x - np.min(x)) / (rng + 1e-9)
    cands["cost_n"] = mm(cands["cost_pred"])
    cands["eta_n"]  = mm(cands["eta_pred_h"])

    # Score pondéré
    cands["score"] = w_cost*cands["cost_n"] + w_eta*cands["eta_n"] + w_risk*cands["risk"]

    # Ranking
    cands = cands.sort_values("score").reset_index(drop=True)

    st.subheader("Ranking carriers")
    st.dataframe(cands[[
        "carrier","service_level","eta_pred_h","cost_pred","on_time_rate","capacity_score","score"
    ]].rename(columns={
        "carrier":"Transporteur","service_level":"Service",
        "eta_pred_h":"ETA (h)","cost_pred":"Coût prédit",
        "on_time_rate":"Fiabilité","capacity_score":"Capacité","score":"Score"
    }))

    best = cands.iloc[0]
    st.success(f"✅ Recommandation: **{best['carrier']} / {best['service_level']}** "
               f"(ETA ~ {best['eta_pred_h']:.1f} h, coût ~ {best['cost_pred']:.0f}, "
               f"fiabilité {best['on_time_rate']:.0%})")
