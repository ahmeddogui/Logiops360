# server/ml_reco_simple_api.py
import os, json
import numpy as np
import pandas as pd
import joblib
from flask import Blueprint, jsonify, request, current_app
from flask_jwt_extended import jwt_required
from sqlalchemy import text

bp_reco_simple = Blueprint("bp_reco_simple", __name__, url_prefix="/api/ml/reco-simple")

HERE = os.path.dirname(os.path.abspath(__file__))
ETA_MODEL_PATH  = os.path.join(HERE, "models", "eta_carrier_lgbm.joblib")
COST_MODEL_PATH = os.path.join(HERE, "models", "cost_lgbm.joblib")
META_PATH       = os.path.join(HERE, "models", "reco_meta.json")

_ETA = None
_COST = None
_META = None

def _load():
    global _ETA, _COST, _META
    if _META is None:
        with open(META_PATH, "r", encoding="utf-8") as f:
            _META = json.load(f)
    if _ETA is None:
        _ETA = joblib.load(ETA_MODEL_PATH)
    if _COST is None and os.path.exists(COST_MODEL_PATH):
        try:
            _COST = joblib.load(COST_MODEL_PATH)
        except Exception:
            _COST = None

# Features d’entrée modèle
FEATURES = [
    "origin","destination_zone","carrier","service_level","ship_dow","ship_hour",
    "distance_km","weight_kg","volume_m3","total_units","n_lines",
    "p50_eta_h","p90_eta_h","delay_rate",
    "cp_cost_baseline_eur","on_time_rate","capacity_score"
]

def _mm(x):
    """Min-max normalisation robuste, même si vecteur constant."""
    x = np.asarray(x, dtype=float)
    if x.size == 0:
        return x
    mn, mx = np.nanmin(x), np.nanmax(x)
    rng = mx - mn
    if not np.isfinite(rng) or rng < 1e-12:
        return np.zeros_like(x, dtype=float)
    return (x - mn) / (rng + 1e-9)

# ---------------------------- Helpers “toujours une reco” ----------------------------

_SERVICE_ALIASES = {
    "LOCAL": {"LOCAL","DOMESTIC","NATIONAL","NATIONALE","FRANCE"},
    "CROSSBORDER": {"CROSSBORDER","INTERNATIONAL","EU","INTL"},
    "EXPRESS": {"EXPRESS","PRIORITY","FAST"},
    "ECONOMY": {"ECONOMY","STANDARD"},
}

def _normalize_svclvl(s: str) -> str:
    if not s: return ""
    s2 = s.strip().upper()
    for canon, bag in _SERVICE_ALIASES.items():
        if s2 in bag:
            return canon
    return s2  # inconnu → on le garde tel quel

def _alias_bag(s: str) -> set:
    """Renvoie l’ensemble d’alias acceptés pour ce service."""
    s2 = _normalize_svclvl(s)
    for canon, bag in _SERVICE_ALIASES.items():
        if s2 == canon:
            return bag
    # Si service inconnu → on essaie tel quel + quelques aliases génériques
    return {s2, "STANDARD", "ECONOMY", "EXPRESS"}

def _fetch_global_lane_medians(eng):
    """
    Médianes globales de secours depuis fv_lane_carrier_stats (toutes lanes confondues).
    """
    q = text("""
        SELECT
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p50_eta_h) AS med_p50,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p90_eta_h) AS med_p90,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delay_rate) AS med_delay
        FROM fv_lane_carrier_stats
    """)
    with eng.connect() as c:
        r = pd.read_sql(q, c)
    if r.empty:
        return {"med_p50": 0.0, "med_p90": 0.0, "med_delay": 0.1}
    x = r.iloc[0].to_dict()
    return {
        "med_p50": float(x.get("med_p50") or 0.0),
        "med_p90": float(x.get("med_p90") or 0.0),
        "med_delay": float(x.get("med_delay") or 0.1),
    }

def _fill_defaults(df: pd.DataFrame, fallbacks: dict, dist: float, wt: float) -> pd.DataFrame:
    """
    Remplit les colonnes manquantes/NaN avec des valeurs par défaut robustes.
    """
    d = df.copy()
    # on_time_rate: 1 - exception_rate déjà calculé côté SQL quand possible ; défaut 0.85
    if "on_time_rate" not in d.columns:
        d["on_time_rate"] = 0.85
    d["on_time_rate"] = pd.to_numeric(d["on_time_rate"], errors="coerce").fillna(0.85).clip(0.0, 1.0)

    # p50/p90/delay_rate: médianes globales si NaN
    for col, fb in [("p50_eta_h", fallbacks["med_p50"]),
                    ("p90_eta_h", fallbacks["med_p90"]),
                    ("delay_rate", fallbacks["med_delay"])]:
        if col not in d.columns:
            d[col] = fb
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(fb)

    # cp_cost_baseline_eur: base_rate_per_km * dist + surcharge_per_kg * wt (si pas déjà fourni)
    if "cp_cost_baseline_eur" not in d.columns:
        d["cp_cost_baseline_eur"] = 0.0
    d["cp_cost_baseline_eur"] = pd.to_numeric(d["cp_cost_baseline_eur"], errors="coerce").fillna(0.0)
    # capacity_score défaut 1.0
    if "capacity_score" not in d.columns:
        d["capacity_score"] = 1.0
    d["capacity_score"] = pd.to_numeric(d["capacity_score"], errors="coerce").fillna(1.0)

    # distance / weight garanties
    d["distance_km"] = float(dist)
    d["weight_kg"] = float(wt)

    # ship_dow / ship_hour / volume / units / lines défauts raisonnables
    for col, val in [("ship_dow", 2), ("ship_hour", 10), ("volume_m3", 0.0), ("total_units", 0), ("n_lines", 0)]:
        if col not in d.columns:
            d[col] = val
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(val)

    return d

# --------------------------------- API ---------------------------------

@bp_reco_simple.get("/distincts")
@jwt_required()
def distincts():
    """Options pour listes déroulantes."""
    eng = current_app.config.get("_ENGINE")
    q1 = "SELECT DISTINCT origin FROM shipments WHERE origin IS NOT NULL ORDER BY 1 LIMIT 500"
    q2 = "SELECT DISTINCT destination_zone FROM shipments WHERE destination_zone IS NOT NULL ORDER BY 1 LIMIT 500"
    q3 = "SELECT DISTINCT service_level FROM carrier_profiles WHERE service_level IS NOT NULL ORDER BY 1 LIMIT 200"
    with eng.connect() as c:
        o = pd.read_sql(text(q1), c)["origin"].astype(str).tolist()
        d = pd.read_sql(text(q2), c)["destination_zone"].astype(str).tolist()
        s = pd.read_sql(text(q3), c)["service_level"].astype(str).tolist()
    return jsonify({"origin": o, "destination_zone": d, "service_level": s})

@bp_reco_simple.post("/recommend")
@jwt_required()
def recommend():
    """
    Body attendu (simple):
    {
      "origin": "...", "destination_zone": "...", "service_level": "...",
      "distance_km": 500, "weight_kg": 120, "volume_m3": 1.2,
      "total_units": 10, "n_lines": 3, "ship_dow": 2, "ship_hour": 10,
      "topk": 5
    }
    Retourne: best (top 1) + topK (liste) avec score combiné coût+ETA+risque.
    Garantit une reco même si la lane/service exact n’existe pas (fallbacks).
    """
    _load()
    eng = current_app.config.get("_ENGINE")
    data = request.get_json(force=True) or {}

    required = ["origin","destination_zone","service_level","distance_km","weight_kg"]
    miss = [k for k in required if k not in data]
    if miss:
        return jsonify(message=f"Champs manquants: {miss}"), 400

    # Poids du score (inchangés)
    w_cost, w_eta, w_risk = 0.5, 0.35, 0.15
    topk = int(data.get("topk", 5))

    origin = (data["origin"] or "").strip()
    dest   = (data["destination_zone"] or "").strip()
    svc_in = (data["service_level"] or "").strip()

    # Normalisation / alias service
    svc_canon = _normalize_svclvl(svc_in)
    svc_aliases = _alias_bag(svc_in)

    dist = float(data.get("distance_km", 0.0))
    wt   = float(data.get("weight_kg", 0.0))

    # Médianes globales de secours
    glb = _fetch_global_lane_medians(eng)

    diagnostics = {"stage": None}

    # ----------------- STAGE 1: candidats stricts lane + service (alias) -----------------
    q1 = text("""
        SELECT
          :origin AS origin, :dest AS destination_zone,
          cp.carrier,
          cp.service_level,
          (1.0 - COALESCE(cp.exception_rate, 0.15))::double precision AS on_time_rate,
          (COALESCE(cp.base_rate_per_km,0) * :dist
           + COALESCE(cp.surcharge_per_kg,0) * :wt)::double precision AS cp_cost_baseline_eur,
          1.0::double precision AS capacity_score,
          lcs.p50_eta_h, lcs.p90_eta_h, lcs.delay_rate
        FROM carrier_profiles cp
        LEFT JOIN fv_lane_carrier_stats lcs
          ON lcs.carrier=cp.carrier
         AND lcs.service_level=cp.service_level
         AND UPPER(lcs.origin)=UPPER(:origin)
         AND UPPER(lcs.destination_zone)=UPPER(:dest)
        WHERE UPPER(TRIM(cp.service_level)) = ANY(:svcalias)
    """)
    with eng.connect() as c:
        cands = pd.read_sql(
            q1, c,
            params={
                "origin": origin, "dest": dest, "dist": dist, "wt": wt,
                "svcalias": list(svc_aliases)
            }
        )

    if cands.empty:
        diagnostics["stage"] = "fallback_lane_agnostic_same_service"
        # -------- STAGE 2: même service (alias), stats “globales” par carrier+service --------
        q2 = text("""
            SELECT
              :origin AS origin, :dest AS destination_zone,
              cp.carrier,
              cp.service_level,
              (1.0 - COALESCE(cp.exception_rate, 0.15))::double precision AS on_time_rate,
              (COALESCE(cp.base_rate_per_km,0) * :dist
               + COALESCE(cp.surcharge_per_kg,0) * :wt)::double precision AS cp_cost_baseline_eur,
              1.0::double precision AS capacity_score,
              -- agrégation globale par carrier+service
              AVG(lcs.p50_eta_h) AS p50_eta_h,
              AVG(lcs.p90_eta_h) AS p90_eta_h,
              AVG(lcs.delay_rate) AS delay_rate
            FROM carrier_profiles cp
            LEFT JOIN fv_lane_carrier_stats lcs
              ON lcs.carrier=cp.carrier
             AND lcs.service_level=cp.service_level
            WHERE UPPER(TRIM(cp.service_level)) = ANY(:svcalias)
            GROUP BY cp.carrier, cp.service_level, cp.exception_rate, cp.base_rate_per_km, cp.surcharge_per_kg
        """)
        with eng.connect() as c:
            cands = pd.read_sql(
                q2, c,
                params={"origin": origin, "dest": dest, "dist": dist, "wt": wt, "svcalias": list(svc_aliases)}
            )

    if cands.empty:
        diagnostics["stage"] = "fallback_any_carrier_any_service"
        # -------- STAGE 3: n'importe quel transporteur (service ignoré), stats globales par carrier --------
        q3 = text("""
            SELECT
              :origin AS origin, :dest AS destination_zone,
              cp.carrier,
              cp.service_level,
              (1.0 - COALESCE(cp.exception_rate, 0.15))::double precision AS on_time_rate,
              (COALESCE(cp.base_rate_per_km,0) * :dist
               + COALESCE(cp.surcharge_per_kg,0) * :wt)::double precision AS cp_cost_baseline_eur,
              1.0::double precision AS capacity_score,
              -- stats moyennes multi-services par carrier
              AVG(lcs.p50_eta_h) AS p50_eta_h,
              AVG(lcs.p90_eta_h) AS p90_eta_h,
              AVG(lcs.delay_rate) AS delay_rate
            FROM carrier_profiles cp
            LEFT JOIN fv_lane_carrier_stats lcs
              ON lcs.carrier=cp.carrier
            GROUP BY cp.carrier, cp.service_level, cp.exception_rate, cp.base_rate_per_km, cp.surcharge_per_kg
        """)
        with eng.connect() as c:
            cands = pd.read_sql(
                q3, c,
                params={"origin": origin, "dest": dest, "dist": dist, "wt": wt}
            )

    if cands.empty:
        # Si vraiment personne en base → renvoie un message explicite plutôt que 404
        return jsonify(message="Aucun transporteur disponible dans carrier_profiles."), 200

    # Remplir valeurs par défaut robustes
    cands = _fill_defaults(cands, glb, dist, wt)

    # Injecter inputs front manquants (pour coller au schéma FEATURES)
    # NB : origin/dest/svc sont déjà présents dans cands
    cands["volume_m3"]   = float(data.get("volume_m3", 0.0))
    cands["total_units"] = int(data.get("total_units", 0))
    cands["n_lines"]     = int(data.get("n_lines", 0))
    cands["ship_dow"]    = int(data.get("ship_dow", 2))
    cands["ship_hour"]   = int(data.get("ship_hour", 10))

    # Prédictions
    X = cands[FEATURES].copy()
    eta_pred  = _ETA.predict(X)
    if _COST is not None:
        try:
            cost_pred = _COST.predict(X)
        except Exception:
            cost_pred = cands["cp_cost_baseline_eur"].to_numpy()
    else:
        cost_pred = cands["cp_cost_baseline_eur"].to_numpy()

    risk = 1.0 - cands["on_time_rate"].to_numpy()

    # Score avec normalisation robuste
    cost_n = _mm(cost_pred)
    eta_n  = _mm(eta_pred)
    risk_n = _mm(risk)  # pour être homogène, même si risk est déjà [0..1] en pratique

    score  = w_cost*cost_n + w_eta*eta_n + w_risk*risk_n

    out = cands.copy()
    out["cost_pred"]  = cost_pred
    out["eta_pred_h"] = eta_pred
    out["risk"]       = risk
    out["score"]      = score

    out = out.sort_values("score", ascending=True).reset_index(drop=True)
    top = out.head(topk).to_dict(orient="records")
    best = top[0] if top else None

    return jsonify({
        "weights": {"cost": w_cost, "eta": w_eta, "risk": w_risk},
        "best": best,
        "topK": top,
        "diagnostics": {
            "origin": origin,
            "destination_zone": dest,
            "service_level_input": svc_in,
            "service_level_used": svc_canon,
            "stage": diagnostics.get("stage") or "lane_exact",
            "fallback_medians": glb,
            "n_candidates": int(len(out)),
        }
    })
