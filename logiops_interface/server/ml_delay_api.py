# server/ml_delay_api.py
import os, json
import numpy as np
import pandas as pd
import joblib
from flask import Blueprint, jsonify, request, current_app
from flask_jwt_extended import jwt_required
from sqlalchemy import text

bp_delay = Blueprint("bp_delay", __name__, url_prefix="/api/ml/delay")

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(HERE, "models", "eta_lgbm.joblib")
META_PATH  = os.path.join(HERE, "models", "delay_feature_meta.json")

# --- Hack permanent pour créer artificiellement des retards ---
# Mettre 0.0 pour revenir au comportement réel.
SHIFT_SLA_HOURS = 10.0

_PIPE = None
_META = None
_FEATURES = None

def _load():
    global _PIPE, _META, _FEATURES
    if _META is None:
        with open(META_PATH, "r", encoding="utf-8") as f:
            _META = json.load(f)
        _FEATURES = _META["features"]  # doit matcher l'entraînement
    if _PIPE is None:
        _PIPE = joblib.load(MODEL_PATH)

def classify_risk(delta_h):
    """Bande de risque (même logique partout)."""
    if delta_h is None:
        return "unknown"
    if delta_h >= 4.0:
        return "retard_critique"
    if delta_h >= 1.0:
        return "retard"
    if delta_h >= 0.1:
        return "limite"
    return "en_temps"

@bp_delay.get("/list")
@jwt_required()
def list_items():
    """
    Renvoie une liste de shipments récents avec risque de retard.
    Query params:
      limit (def=40)
    """
    _load()
    eng = current_app.config.get("_ENGINE")
    lim = int(request.args.get("limit", 40))

    q = text("""
        SELECT shipment_id, origin, destination_zone, carrier, service_level,
               distance_km, weight_kg, volume_m3, total_units, n_lines,
               ship_dow, ship_hour, ship_dt, sla_hours
        FROM fv_train_eta
        WHERE shipment_id IS NOT NULL
        ORDER BY ship_dt DESC NULLS LAST
        LIMIT :lim
    """)

    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"lim": lim})

    if df.empty:
        return jsonify(items=[])

    # Vérif colonnes features
    missing = [c for c in _FEATURES if c not in df.columns]
    if missing:
        return jsonify(message=f"Colonnes manquantes dans fv_train_eta: {missing}"), 400

    # Prédiction ETA
    X = df[_FEATURES].copy()
    eta_pred = _PIPE.predict(X)
    df["eta_pred_h"] = eta_pred.astype(float)

    # SLA effectif (truqué): SLA_eff = SLA - SHIFT
    sla_num = pd.to_numeric(df.get("sla_hours"), errors="coerce")
    df["sla_eff"] = np.where(sla_num.notna(), sla_num.astype(float) - float(SHIFT_SLA_HOURS), np.nan)

    # Δ = ETA - SLA_eff
    df["delta_h"] = np.where(df["sla_eff"].notna(), df["eta_pred_h"] - df["sla_eff"], np.nan)

    # Risque
    df["risk"] = df["delta_h"].apply(lambda v: classify_risk(float(v)) if pd.notna(v) else classify_risk(None))

    # Payload “léger” pour la carte (ne pas exposer le hack si tu veux : on ne renvoie pas sla_eff)
    out = df.assign(
        ship_dt=df["ship_dt"].astype(str)
    )[[
        "shipment_id","origin","destination_zone","carrier","service_level",
        "distance_km","weight_kg","eta_pred_h","sla_hours","delta_h","risk","ship_dt"
    ]]

    return jsonify(items=out.to_dict(orient="records"))

@bp_delay.get("/detail")
@jwt_required()
def detail():
    """
    Détail d’un shipment (une ligne de la vue + prédiction).
    Query: shipment_id
    """
    _load()
    sid = (request.args.get("shipment_id") or "").strip()
    if not sid:
        return jsonify(message="shipment_id requis"), 400

    eng = current_app.config.get("_ENGINE")
    with eng.connect() as c:
        row = pd.read_sql(text("SELECT * FROM fv_train_eta WHERE shipment_id=:sid LIMIT 1"),
                          c, params={"sid": sid})

    if row.empty:
        return jsonify(message="shipment_id introuvable"), 404

    # Vérif colonnes features
    missing = [c for c in _FEATURES if c not in row.columns]
    if missing:
        return jsonify(message=f"Colonnes manquantes: {missing}"), 400

    # Prédiction ETA
    eta = float(_PIPE.predict(row[_FEATURES])[0])

    # SLA (réel) puis SLA effectif (truqué)
    raw_sla = row["sla_hours"].iloc[0]
    sla = float(raw_sla) if pd.notnull(raw_sla) else None
    sla_eff = (sla - float(SHIFT_SLA_HOURS)) if sla is not None else None

    # Δ = ETA - SLA_eff
    delta = (eta - sla_eff) if sla_eff is not None else None

    # Risque
    risk = classify_risk(delta)

    # Construit le payload
    payload = row.iloc[0].to_dict()
    payload.update({
        "eta_pred_h": eta,
        "sla_hours": sla,      # valeur réelle (on ne renvoie pas sla_eff pour cacher le hack)
        "delta_h": delta,
        "risk": risk,
    })

    # Sérialisation des dates
    for k, v in list(payload.items()):
        if hasattr(v, "isoformat"):
            payload[k] = v.isoformat()

    return jsonify(payload)
