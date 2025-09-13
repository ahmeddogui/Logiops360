# create_view_reco.py
from sqlalchemy import create_engine, text
import os, sys

# Connexion DB (tes valeurs par défaut)
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")

sql = """
-- =======================================================
-- 1) Stats historiques par lane + carrier
--    (médiane ETA, P90, taux de retard vs SLA transporteur)
-- =======================================================
CREATE OR REPLACE VIEW fv_lane_carrier_stats AS
WITH base AS (
  SELECT
    s.origin,
    s.destination_zone,
    s.carrier,
    s.service_level,
    EXTRACT(EPOCH FROM ((s.delivery_datetime)::timestamptz - (s.ship_datetime)::timestamptz))/3600.0 AS eta_h,
    CASE
      WHEN cp.sla_hours IS NULL THEN NULL
      WHEN (s.delivery_datetime)::timestamptz - (s.ship_datetime)::timestamptz
           > (cp.sla_hours || ' hours')::interval THEN 1
      ELSE 0
    END AS is_late
  FROM shipments s
  LEFT JOIN carrier_profiles cp
    ON cp.carrier = s.carrier AND cp.service_level = s.service_level
  WHERE s.delivery_datetime IS NOT NULL
)
SELECT
  origin,
  destination_zone,
  carrier,
  service_level,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY eta_h) AS p50_eta_h,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY eta_h) AS p90_eta_h,
  AVG(is_late)::float AS delay_rate
FROM base
GROUP BY origin, destination_zone, carrier, service_level;

-- =======================================================
-- 2) Vue d’entraînement reco (ETA & coût)
--    - coût historique = shipments.cost_estimated
--    - proxies depuis carrier_profiles:
--         on_time_rate = 1 - exception_rate
--         cp_cost_baseline_eur = base_rate_per_km*distance + surcharge_per_kg*weight
--         capacity_score = 1.0 (placeholder)
-- =======================================================
CREATE OR REPLACE VIEW fv_train_carrier_choice AS
SELECT
  s.shipment_id,
  s.origin,
  s.destination_zone,
  s.carrier,
  s.service_level,
  s.distance_km,
  s.weight_kg,
  s.volume_m3,
  s.total_units,
  s.n_lines,

  -- Cibles d’entraînement
  EXTRACT(EPOCH FROM ((s.delivery_datetime)::timestamptz - (s.ship_datetime)::timestamptz))/3600.0 AS eta_h,
  s.cost_estimated::double precision AS actual_total_cost_eur,

  -- Features dérivées/proxies
  lcs.p50_eta_h,
  lcs.p90_eta_h,
  lcs.delay_rate,
  (1.0 - COALESCE(cp.exception_rate, 0.15))::double precision AS on_time_rate,
  (COALESCE(cp.base_rate_per_km,0) * COALESCE(s.distance_km,0)
   + COALESCE(cp.surcharge_per_kg,0) * COALESCE(s.weight_kg,0))::double precision AS cp_cost_baseline_eur,
  1.0::double precision AS capacity_score,

  DATE_TRUNC('day', (s.ship_datetime)::timestamptz)::date AS ship_day,
  EXTRACT(DOW  FROM (s.ship_datetime)::timestamptz)::int  AS ship_dow,
  EXTRACT(HOUR FROM (s.ship_datetime)::timestamptz)::int AS ship_hour

FROM shipments s
LEFT JOIN fv_lane_carrier_stats lcs
  ON lcs.origin = s.origin
 AND lcs.destination_zone = s.destination_zone
 AND lcs.carrier = s.carrier
 AND lcs.service_level = s.service_level
LEFT JOIN carrier_profiles cp
  ON cp.carrier = s.carrier
 AND cp.service_level = s.service_level
WHERE s.delivery_datetime IS NOT NULL;

-- =======================================================
-- 3) Candidats pour ranking (toutes options par lane/service)
--    (on calcule déjà les proxies coût/fiabilité côté vue)
-- =======================================================
CREATE OR REPLACE VIEW fv_reco_candidates AS
SELECT DISTINCT
  s.origin,
  s.destination_zone,
  cp.carrier,
  cp.service_level,
  (1.0 - COALESCE(cp.exception_rate, 0.15))::double precision AS on_time_rate,
  (COALESCE(cp.base_rate_per_km,0) * COALESCE(s.distance_km,0)
   + COALESCE(cp.surcharge_per_kg,0) * COALESCE(s.weight_kg,0))::double precision AS cp_cost_baseline_eur,
  1.0::double precision AS capacity_score
FROM shipments s
JOIN carrier_profiles cp ON TRUE;
"""

def main():
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

            n1 = conn.execute(text("SELECT COUNT(*) FROM fv_lane_carrier_stats")).scalar()
            n2 = conn.execute(text("SELECT COUNT(*) FROM fv_train_carrier_choice")).scalar()
            n3 = conn.execute(text("SELECT COUNT(*) FROM fv_reco_candidates")).scalar()

        print("✅ Vues créées/mises à jour :")
        print(f"   • fv_lane_carrier_stats      (lignes: {n1})")
        print(f"   • fv_train_carrier_choice    (lignes: {n2})")
        print(f"   • fv_reco_candidates         (lignes: {n3})")

    except Exception as e:
        print("❌ Erreur lors de la création des vues :", e, file=sys.stderr)
        raise

if __name__ == "__main__":
    main()
