from sqlalchemy import create_engine, text
import os

# ---- Connexion DB (mêmes valeurs que précédemment, adaptables par variables d'env) ----
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "313055")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "logiops")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")

# ---- Vue: on repart de fv_train_eta et on crée la cible binaire is_late ----
sql = """
CREATE OR REPLACE VIEW fv_train_delay AS
SELECT
    shipment_id,
    ordernumber,
    origin,
    destination_zone,
    carrier,
    service_level,
    distance_km,
    weight_kg,
    volume_m3,
    total_units,
    n_lines,
    ship_dt,
    ship_day,          -- idéalement de type date dans fv_train_eta
    ship_dow,
    ship_hour,
    sla_hours,
    target_eta_hours,
    CASE
        WHEN target_eta_hours IS NULL OR sla_hours IS NULL THEN NULL
        WHEN target_eta_hours > sla_hours THEN 1
        ELSE 0
    END AS is_late
FROM fv_train_eta
WHERE target_eta_hours IS NOT NULL
  AND sla_hours IS NOT NULL;
"""

def main():
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
        print("✅ Vue fv_train_delay créée avec succès.")
        count = conn.execute(text("SELECT COUNT(*) FROM fv_train_delay")).scalar()
        print(f"ℹ️ Lignes prêtes pour l'entraînement: {count}")

if __name__ == "__main__":
    main()
