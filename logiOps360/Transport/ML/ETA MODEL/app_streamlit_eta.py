# app_streamlit_eta.py
# ------------------------------------------------------------
# Demo Streamlit : Modèle ETA
# - charge le modèle: eta_lgbm.joblib
# - lit la vue Postgres: fv_train_eta
# - permet de choisir un shipment_id et affiche ETA + arrivée estimée + delta SLA
# ------------------------------------------------------------

import os
from datetime import timedelta
import pytz
import pandas as pd
import joblib
import streamlit as st
from sqlalchemy import create_engine, text

# =========================
# Config (adapte si besoin)
# =========================
# Creds Postgres (utilise env vars si définies)
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")

DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Chemins des artefacts (même dossier que ce fichier)
APP_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(APP_DIR, "eta_lgbm.joblib")
META_PATH  = os.path.join(APP_DIR, "eta_feature_meta.json")

# Liste de features (doit matcher l'entraînement)
FEATURES = [
    "origin", "destination_zone", "carrier", "service_level",
    "ship_dow", "ship_hour",
    "distance_km", "weight_kg", "volume_m3", "total_units", "n_lines",
]

# Colonne date & champs utiles
SHIP_DT_COL = "ship_dt"
SLA_COL     = "sla_hours"
TARGET_COL  = "target_eta_hours"  # pour debug/contrôle


# =========================
# App setup
# =========================
st.set_page_config(page_title="LogiOps360 • ETA Predictor", layout="wide")
st.title("🚚 ETA Predictor (V1)")
st.caption("Modèle LightGBM entraîné sur la vue `fv_train_eta`")

# =========================
# Helpers (cache)
# =========================
@st.cache_resource
def get_engine():
    return create_engine(DB_URI)

@st.cache_resource
def load_model():
    return joblib.load(MODEL_PATH)

@st.cache_data(ttl=300)
def load_view(limit: int = 2000) -> pd.DataFrame:
    """Charge un extrait de la vue pour peupler la liste des shipments & features."""
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM fv_train_eta ORDER BY {SHIP_DT_COL} DESC LIMIT :lim"),
                         conn, params={"lim": limit})
    # Normalise types
    # ship_dt (tz-aware) -> naive local Europe/Paris pour l'affichage
    if SHIP_DT_COL in df.columns:
        s = pd.to_datetime(df[SHIP_DT_COL], utc=True, errors="coerce")
        df[SHIP_DT_COL] = s.dt.tz_convert("Europe/Paris").dt.tz_localize(None)
    return df

@st.cache_data(ttl=300)
def get_row_by_id(shipment_id: str) -> pd.Series:
    eng = get_engine()
    with eng.connect() as conn:
        q = text("SELECT * FROM fv_train_eta WHERE shipment_id = :sid")
        df = pd.read_sql(q, conn, params={"sid": shipment_id})
    if df.empty:
        raise ValueError(f"Shipment '{shipment_id}' introuvable dans fv_train_eta.")
    # normalise date
    if SHIP_DT_COL in df.columns:
        s = pd.to_datetime(df[SHIP_DT_COL], utc=True, errors="coerce")
        df[SHIP_DT_COL] = s.dt.tz_convert("Europe/Paris").dt.tz_localize(None)
    return df.iloc[0]


# =========================
# Main UI
# =========================
colL, colR = st.columns([1.2, 1], vertical_alignment="center")

with colL:
    df_list = load_view()
    st.subheader("Sélection d’expédition")
    if df_list.empty:
        st.error("La vue `fv_train_eta` ne contient pas de lignes.")
        st.stop()

    # choix ID
    shipment_ids = df_list["shipment_id"].astype(str).tolist()
    chosen_id = st.selectbox("Shipment ID", shipment_ids, index=0)

    # bouton refresh
    if st.button("🔄 Rafraîchir la vue", help="Recharge les données récentes de fv_train_eta"):
        st.cache_data.clear()
        df_list = load_view()
        st.success("Vue rechargée.")

with colR:
    st.info("💡 Astuce : tu peux basculer en mode 'What-If' pour simuler un autre carrier/service_level, etc.")

st.divider()

# Chargement modèle
try:
    model = load_model()
except Exception as e:
    st.error(f"Impossible de charger le modèle: {e}\nAttendu: {MODEL_PATH}")
    st.stop()

# Sélecteur de mode (by id / what-if)
mode = st.radio("Mode de prédiction", ["Par Shipment ID", "What-If (manuel)"], horizontal=True)

if mode == "Par Shipment ID":
    try:
        row = get_row_by_id(chosen_id)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Build X (1 ligne) avec les features d'entraînement
    missing = [c for c in FEATURES if c not in row.index]
    if missing:
        st.error(f"Colonnes manquantes dans fv_train_eta: {missing}")
        st.stop()

    X = row[FEATURES].to_frame().T
    try:
        eta_h = float(model.predict(X)[0])
    except Exception as e:
        st.error(f"Erreur de prédiction: {e}")
        st.stop()

    # Calculs d’affichage
    ship_dt = pd.to_datetime(row[SHIP_DT_COL])
    eta_dt  = ship_dt + timedelta(hours=eta_h)
    sla_h   = float(row[SLA_COL]) if pd.notnull(row.get(SLA_COL, None)) else None
    sla_dt  = (ship_dt + timedelta(hours=sla_h)) if sla_h is not None else None
    delta_h = (eta_h - sla_h) if sla_h is not None else None

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("ETA prévu (h)", f"{eta_h:.2f}")
    k2.metric("Arrivée estimée (Europe/Paris)", eta_dt.strftime("%Y-%m-%d %H:%M"))
    if sla_h is not None:
        k3.metric("SLA vs ETA (h)", f"{delta_h:+.2f}")

    # Badges/état
    if sla_h is not None:
        if delta_h > 0.2:
            st.error(f"⚠️ Risque de dépassement SLA d’environ {delta_h:.2f} h")
        elif delta_h > -0.2:
            st.warning("🟨 À la limite du SLA")
        else:
            st.success("✅ Dans le SLA")

    # Détails
    with st.expander("Détails expédition"):
        meta_cols = ["origin","destination_zone","carrier","service_level",
                     "distance_km","weight_kg","volume_m3","total_units","n_lines",
                     "ship_dow","ship_hour", SLA_COL, SHIP_DT_COL]
        st.write(row[meta_cols])

    # (Optionnel) comparer au réel si livré (target)
    if pd.notnull(row.get(TARGET_COL, None)):
        with st.expander("Contrôle (historique)"):
            st.write(f"ETA observé (target): **{float(row[TARGET_COL]):.2f} h**")

else:
    # What-If form
    st.subheader("What-If : saisie manuelle des features")
    with st.form("what_if"):
        c1, c2, c3 = st.columns(3)
        origin = c1.selectbox("Origin", sorted(df_list["origin"].dropna().unique().tolist()))
        dest   = c2.selectbox("Destination zone", sorted(df_list["destination_zone"].dropna().unique().tolist()))
        carrier = c3.selectbox("Carrier", sorted(df_list["carrier"].dropna().unique().tolist()))
        service = c1.selectbox("Service level", sorted(df_list["service_level"].dropna().unique().tolist()))
        ship_dow = c2.number_input("Ship DOW (0=dim, 6=sam)", min_value=0, max_value=6, value=2)
        ship_hour = c3.number_input("Ship hour (0-23)", min_value=0, max_value=23, value=10)

        distance = c1.number_input("Distance (km)", min_value=0.0, value=500.0, step=10.0)
        weight   = c2.number_input("Weight (kg)", min_value=0.0, value=120.0, step=5.0)
        volume   = c3.number_input("Volume (m3)", min_value=0.0, value=1.2, step=0.1)
        units    = c1.number_input("Total units", min_value=0, value=10, step=1)
        n_lines  = c2.number_input("N lines", min_value=0, value=3, step=1)

        sla_h = c3.number_input("SLA (h) [optionnel]", min_value=0.0, value=72.0, step=1.0)

        submitted = st.form_submit_button("Prédire (What-If)")

    if submitted:
        Xw = pd.DataFrame([{
            "origin": origin,
            "destination_zone": dest,
            "carrier": carrier,
            "service_level": service,
            "ship_dow": int(ship_dow),
            "ship_hour": int(ship_hour),
            "distance_km": float(distance),
            "weight_kg": float(weight),
            "volume_m3": float(volume),
            "total_units": int(units),
            "n_lines": int(n_lines),
        }])

        try:
            eta_h = float(model.predict(Xw)[0])
            st.success(f"ETA prévu: **{eta_h:.2f} h**")
            if sla_h and sla_h > 0:
                delta_h = eta_h - float(sla_h)
                if delta_h > 0.2:
                    st.error(f"⚠️ Risque de dépassement SLA ~{delta_h:.2f} h")
                elif delta_h > -0.2:
                    st.warning("🟨 À la limite du SLA")
                else:
                    st.success("✅ Dans le SLA")
        except Exception as e:
            st.error(f"Erreur de prédiction (What-If): {e}")

st.divider()
st.caption("Modèle: eta_lgbm.joblib • Vue: fv_train_eta • Fuseau: Europe/Paris")
