# server/kpi_api.py
from flask import Blueprint, jsonify, current_app
from flask_jwt_extended import jwt_required
from sqlalchemy import text
import pandas as pd

bp_kpi = Blueprint("bp_kpi", __name__, url_prefix="/api/kpi")

@bp_kpi.get("/counters")
@jwt_required()
def counters():
    """
    Livraisons en cours = nb de shipments dont la dernière phase != delivered.
    """
    eng = current_app.config.get("_ENGINE")

    sql = text("""
        WITH latest AS (
          SELECT
            e.shipment_id,
            FIRST_VALUE(LOWER(TRIM(e.event_type))) OVER (
              PARTITION BY e.shipment_id
              ORDER BY (e.event_time)::timestamptz DESC, e.event_id DESC
            ) AS last_phase
          FROM shipment_events e
        )
        SELECT COUNT(DISTINCT shipment_id) AS in_progress
        FROM latest
        WHERE last_phase IS DISTINCT FROM 'delivered'
    """)
    with eng.connect() as c:
      df = pd.read_sql(sql, c)

    in_progress = int(df["in_progress"].iloc[0]) if not df.empty else 0

    return jsonify({
        "in_progress": in_progress
    })
