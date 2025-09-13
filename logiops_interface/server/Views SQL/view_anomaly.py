from sqlalchemy import create_engine, text
import os
import sys

# --- Connexion DB ---
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "kdh")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

SQL_STATEMENTS = [
    # 0) DROP dans l'ordre inverse des dépendances
    """
DROP VIEW IF EXISTS fv_phase_enriched;
""",
    """
DROP VIEW IF EXISTS fv_phase_stats;
""",
    """
DROP VIEW IF EXISTS fv_phase_durations;
""",

    # 1) Durées brutes : on NE filtre PAS 'delivered' ici
    #    -> ainsi la phase N-1 a comme durée le delta jusqu'à delivered
    """
CREATE OR REPLACE VIEW fv_phase_durations AS
WITH raw AS (
  SELECT
    e.shipment_id,
    e.event_id,
    e.event_type,
    LOWER(TRIM(e.event_type)) AS phase_norm,
    (e.event_time)::timestamptz       AS event_time,
    LEAD((e.event_time)::timestamptz) OVER (
      PARTITION BY e.shipment_id
      ORDER BY (e.event_time)::timestamptz, e.event_id
    ) AS next_time
  FROM shipment_events e
)
SELECT
  shipment_id,
  event_id,
  event_type AS phase,   -- libellé d'origine pour l'UI
  phase_norm,            -- libellé normalisé pour jointures/filtres
  event_time,
  EXTRACT(EPOCH FROM (next_time - event_time))/3600.0 AS duration_h
FROM raw;
""",

    # 2) Stats par (carrier, phase_norm) en EXCLUANT delivered, et en ignorant les NULL
    """
CREATE OR REPLACE VIEW fv_phase_stats AS
SELECT
  s.carrier,
  d.phase_norm AS phase,
  AVG(d.duration_h) AS avg_duration_h,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d.duration_h) AS p50_duration_h,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY d.duration_h) AS p90_duration_h,
  STDDEV(d.duration_h) AS std_duration_h,
  COUNT(*) AS n_obs
FROM fv_phase_durations d
JOIN shipments s ON s.shipment_id = d.shipment_id
WHERE d.duration_h IS NOT NULL
  AND d.phase_norm <> 'delivered'
GROUP BY s.carrier, d.phase_norm;
""",

    # 3) Évènements enrichis (sans delivered) + règle P90
    """
CREATE OR REPLACE VIEW fv_phase_enriched AS
SELECT
  d.shipment_id,
  d.event_id,
  d.phase,               -- libellé d'origine (pour l'UI)
  s.carrier,
  d.duration_h,
  ps.avg_duration_h,
  ps.p50_duration_h,
  ps.p90_duration_h,
  ps.std_duration_h,
  CASE
    WHEN d.duration_h IS NULL THEN NULL
    WHEN d.duration_h > ps.p90_duration_h THEN 1
    ELSE 0
  END AS is_anomaly_rule
FROM fv_phase_durations d
JOIN shipments s ON s.shipment_id = d.shipment_id
LEFT JOIN fv_phase_stats ps
  ON ps.carrier = s.carrier
 AND ps.phase   = d.phase_norm
WHERE d.duration_h IS NOT NULL;
"""
]


def main():
    try:
        with engine.begin() as conn:
            for i, stmt in enumerate(SQL_STATEMENTS, 1):
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    print(f"Erreur à l'étape {i} :", file=sys.stderr)
                    print(stmt, file=sys.stderr)
                    raise

        # Compter les lignes pour feedback
        with engine.connect() as conn:
            n1 = conn.execute(text("SELECT COUNT(*) FROM fv_phase_stats")).scalar()
            n2 = conn.execute(text("SELECT COUNT(*) FROM fv_phase_enriched")).scalar()

        print("Vues créées/mises à jour :")
        print(f"   • fv_phase_stats     (lignes: {n1})")
        print(f"   • fv_phase_enriched  (lignes: {n2})")

    except Exception as e:
        print("Échec lors de la création des vues :", e, file=sys.stderr)
        # le raise te donnera le message précis de Postgres/SQLAlchemy
        raise


if __name__ == "__main__":
    main()
