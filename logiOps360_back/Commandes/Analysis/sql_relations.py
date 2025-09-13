import pandas as pd
import sys
from pathlib import Path
from sqlalchemy import text

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.db_utils import connect_db

engine = connect_db()

query = text("""
CREATE OR REPLACE VIEW vw_orders_details AS
SELECT
    co.*,
    pw.wave_number,
    pw.operator AS picking_operator,
    p.abccod,
    p.sector
FROM clean_customer_orders co
LEFT JOIN clean_picking_wave pw ON co.reference = pw.reference
LEFT JOIN clean_product p ON co.reference = p.reference;
""")


with engine.connect() as conn:
    conn.execute(query)
    
    result = conn.execute(text("""
        SELECT viewname
        FROM pg_catalog.pg_views
        WHERE viewname = 'vw_orders_details';
    """))
    
    view_exists = result.fetchone() is not None
    
    if view_exists:
        print("Vue vw_orders_details créée avec succès.")
    else:
        print("La vue vw_orders_details n'a pas été trouvée.")

