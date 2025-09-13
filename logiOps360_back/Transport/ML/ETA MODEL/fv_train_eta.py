from sqlalchemy import create_engine, text

# Variables de connexion
PG_USER = "postgres"
PG_PASSWORD = "313055"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "logiops"

# Connexion SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

# SQL de création de la vue
sql = """
CREATE OR REPLACE VIEW fv_train_eta AS
SELECT
    s.shipment_id,
    s.ordernumber,
    s.origin,
    s.destination_zone,
    s.carrier,
    s.service_level,
    s.distance_km,
    s.weight_kg,
    s.volume_m3,
    s.total_units,
    s.n_lines,
    (s.ship_datetime)::timestamptz AS ship_dt,
    DATE_TRUNC('day', (s.ship_datetime)::timestamptz) AS ship_day,
    EXTRACT(DOW  FROM (s.ship_datetime)::timestamptz)::int  AS ship_dow,
    EXTRACT(HOUR FROM (s.ship_datetime)::timestamptz)::int AS ship_hour,
    cp.sla_hours,
    EXTRACT(EPOCH FROM (
        (s.delivery_datetime)::timestamptz - (s.ship_datetime)::timestamptz
    )) / 3600.0 AS target_eta_hours
FROM shipments s
LEFT JOIN carrier_profiles cp
  ON cp.carrier = s.carrier
 AND cp.service_level = s.service_level
WHERE s.delivery_datetime IS NOT NULL;
"""

def main():
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
        print(" Vue fv_train_eta créée avec succès.")

        # Vérification rapide : combien de lignes dans la vue
        result = conn.execute(text("SELECT COUNT(*) FROM fv_train_eta;"))
        count = result.scalar()
        print(f"ℹ️ Nombre de lignes dans fv_train_eta : {count}")

if __name__ == "__main__":
    main()
