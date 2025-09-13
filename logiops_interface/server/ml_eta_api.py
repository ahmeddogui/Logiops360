# ml_eta_api.py
import os, json
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required
from sqlalchemy import text
import pandas as pd
import joblib

bp_eta = Blueprint("bp_eta", __name__, url_prefix="/api/ml/eta")

# --- Config / chemins ---
HERE = os.path.dirname(os.path.abspath(__file__))
ETA_MODEL_PATH  = os.path.join(HERE, "models", "eta_lgbm.joblib")
ETA_META_PATH   = os.path.join(HERE, "models", "eta_feature_meta.json")
# --- Artefacts chargés une fois ---
_PIPE = joblib.load(ETA_MODEL_PATH)
with open(ETA_META_PATH, "r", encoding="utf-8") as f:
    _META = json.load(f)
_FEATURES = _META["features"]
_MODEL_VERSION = _META.get("generated_at", "v1")

def _predict_dataframe(df: pd.DataFrame):
    # vérif colonnes & ordre
    missing = [c for c in _FEATURES if c not in df.columns]
    if missing:
        raise ValueError(f"Missing features in payload: {missing}")
    df = df[_FEATURES]
    y = _PIPE.predict(df)
    return [round(float(v), 2) for v in y]

@bp_eta.get("/meta")
@jwt_required(optional=True)  # tu peux exiger le token en mettant jwt_required() sans optional
def meta():
    return jsonify({
        "features": _FEATURES,
        "model_version": _MODEL_VERSION,
        "metrics_test": _META.get("metrics_test"),
        "algo": _META.get("algo"),
    }), 200

@bp_eta.post("/predict")
@jwt_required()  # protège avec JWT comme le reste de ton API
def predict():
    data = request.get_json(force=True) or {}
    items = data.get("items", [])
    if not isinstance(items, list) or len(items) == 0:
        return jsonify(message="Body must contain 'items': [ {...}, ... ]"), 400
    try:
        df = pd.DataFrame(items)
        preds = _predict_dataframe(df)
        return jsonify(eta_hours=preds, n=len(preds), model_version=_MODEL_VERSION), 200
    except Exception as e:
        return jsonify(message="Prediction error", error=str(e)), 400

@bp_eta.get("/predict-by-id")
@jwt_required()
def predict_by_id():
    # query param: ?shipment_id=...
    shipment_id = request.args.get("shipment_id", "").strip()
    if not shipment_id:
        return jsonify(message="shipment_id is required"), 400

    # l'objet engine vient de app.py (on l'injectera via current_app)
    from flask import current_app
    engine = current_app.config.get("_ENGINE")
    if engine is None:
        return jsonify(message="DB engine not available"), 500

    try:
        q = text("SELECT * FROM fv_train_eta WHERE shipment_id = :sid LIMIT 1")
        df = pd.read_sql(q, engine, params={"sid": shipment_id})
        if df.empty:
            return jsonify(message="shipment_id not found in fv_train_eta"), 404
        preds = _predict_dataframe(df)
        return jsonify(shipment_id=shipment_id, eta_hours=preds[0], model_version=_MODEL_VERSION), 200
    except Exception as e:
        return jsonify(message="DB/prediction error", error=str(e)), 400



@bp_eta.get("/distincts")
@jwt_required()
def distincts():
    from flask import current_app
    engine = current_app.config.get("_ENGINE")
    q = """
    SELECT
      ARRAY(SELECT DISTINCT origin FROM fv_train_eta LIMIT 500) AS origin,
      ARRAY(SELECT DISTINCT destination_zone FROM fv_train_eta LIMIT 500) AS destination_zone,
      ARRAY(SELECT DISTINCT carrier FROM fv_train_eta LIMIT 500) AS carrier,
      ARRAY(SELECT DISTINCT service_level FROM fv_train_eta LIMIT 500) AS service_level
    """
    df = pd.read_sql(text(q), engine)
    row = df.iloc[0].to_dict()
    return jsonify({k: [x for x in v if x is not None] for k, v in row.items()})


@bp_eta.get("/shipments")
@jwt_required()
def list_shipments():
    from flask import current_app, request, jsonify
    from sqlalchemy import text
    import pandas as pd

    engine = current_app.config.get("_ENGINE")
    lim = int(request.args.get("limit", 200))

    # ship_dt peut s'appeler autrement chez toi ; adapte si besoin
    q = text("""
        SELECT shipment_id
        FROM fv_train_eta
        WHERE shipment_id IS NOT NULL
        ORDER BY ship_dt DESC NULLS LAST
        LIMIT :lim
    """)
    df = pd.read_sql(q, engine, params={"lim": lim})
    ids = df["shipment_id"].astype(str).tolist()
    return jsonify({"shipment_ids": ids})
