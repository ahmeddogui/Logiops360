# server/ml_anomaly_api.py
import math
from flask import Blueprint, jsonify, request, current_app
from flask_jwt_extended import jwt_required
import pandas as pd
from sqlalchemy import text

bp_anom = Blueprint("bp_anom", __name__, url_prefix="/api/ml/anom")

# --- helpers ---
def severity_from_ratio(ratio: float) -> str:
    """Bucket simple selon l’écart au P90 (ratio = duration_h / p90_duration_h)."""
    if ratio is None or math.isnan(ratio) or math.isinf(ratio):
        return "inconnu"
    if ratio >= 1.5:
        return "haute"
    if ratio >= 1.1:
        return "moyenne"
    return "basse"  # >P90 mais léger (ex : 1.00–1.10)

# --------- LIST ----------
@bp_anom.get("/list")
@jwt_required()
def list_anomalies():
    """
    Retourne les anomalies P90 récentes (une tuile par événement anormal).
    Query:
      - limit (int, def=30)
    """
    eng = current_app.config.get("_ENGINE")
    lim = int(request.args.get("limit", 30))

    q = text("""
        SELECT
          e.shipment_id,
          e.event_id,
          e.phase,
          s.carrier,
          e.duration_h,
          e.avg_duration_h,
          e.p50_duration_h,
          e.p90_duration_h,
          s.origin,
          s.destination_zone,
          s.distance_km,
          s.weight_kg
        FROM fv_phase_enriched e
        JOIN shipments s ON s.shipment_id = e.shipment_id
        WHERE e.is_anomaly_rule = 1             -- uniquement les > P90
          AND e.duration_h IS NOT NULL
          AND e.p90_duration_h IS NOT NULL
        ORDER BY e.event_id DESC
        LIMIT :lim
    """)

    with eng.connect() as c:
        df = pd.read_sql(q, c, params={"lim": lim})

    if df.empty:
        return jsonify(items=[])

    # ratio / p90 + sévérité
    df["ratio_p90"] = df["duration_h"] / df["p90_duration_h"]
    df["severity"] = df["ratio_p90"].apply(severity_from_ratio)

    cols = [
        "shipment_id","event_id","phase","carrier",
        "origin","destination_zone","distance_km","weight_kg", 
        "duration_h","avg_duration_h","p50_duration_h","p90_duration_h",
        "ratio_p90","severity"
    ]
    out = df[cols].copy()

    return jsonify(items=out.to_dict(orient="records"))

# --------- DETAIL ----------
@bp_anom.get("/detail")
@jwt_required()
def detail():
    """
    Détail d’une anomalie.
    Query:
      - shipment_id (str)
      - event_id (int)
    """
    shipment_id = (request.args.get("shipment_id") or "").strip()
    event_id     = request.args.get("event_id")

    if not shipment_id or event_id is None:
        return jsonify(message="shipment_id et event_id requis"), 400

    eng = current_app.config.get("_ENGINE")

    # 1) L’évènement anormal
    q1 = text("""
        SELECT
          e.*,
          s.origin, s.destination_zone, s.carrier, s.distance_km, s.weight_kg
        FROM fv_phase_enriched e
        JOIN shipments s ON s.shipment_id = e.shipment_id
        WHERE e.shipment_id = :sid AND e.event_id = :eid
        LIMIT 1
    """)
    with eng.connect() as c:
        row = pd.read_sql(q1, c, params={"sid": shipment_id, "eid": int(event_id)})

    if row.empty:
        return jsonify(message="anomalie introuvable"), 404

    r = row.iloc[0].to_dict()
    ratio_p90 = None
    try:
        ratio_p90 = float(r["duration_h"]) / float(r["p90_duration_h"]) if r.get("p90_duration_h") else None
    except Exception:
        ratio_p90 = None

    # 2) Les 4–6 dernières phases de ce shipment (contexte)
    q2 = text("""
        SELECT event_id, phase, duration_h, avg_duration_h, p50_duration_h, p90_duration_h
        FROM fv_phase_enriched
        WHERE shipment_id = :sid
        ORDER BY event_id ASC
        LIMIT 100
    """)
    with eng.connect() as c:
        phases = pd.read_sql(q2, c, params={"sid": shipment_id})

    # marquer la phase en anomalie
    phases["is_anom_p90"] = (phases["duration_h"] > phases["p90_duration_h"]).astype(int)

    payload = {
        "shipment_id": r.get("shipment_id"),
        "event_id": int(r.get("event_id")),
        "phase": r.get("phase"),
        "carrier": r.get("carrier"),
        "origin": r.get("origin"),
        "destination_zone": r.get("destination_zone"),
        "distance_km": r.get("distance_km"),
        "weight_kg": r.get("weight_kg"),
        "duration_h": r.get("duration_h"),
        "avg_duration_h": r.get("avg_duration_h"),
        "p50_duration_h": r.get("p50_duration_h"),
        "p90_duration_h": r.get("p90_duration_h"),
        "ratio_p90": ratio_p90,
        "severity": severity_from_ratio(ratio_p90),
        "phases": phases.to_dict(orient="records")
    }
    return jsonify(payload)
