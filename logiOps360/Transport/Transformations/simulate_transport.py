#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simulate transport data from orders and load into PostgreSQL.
Patched v3:
 - Keep robust casting of identifiers to string (ordernumber, codcustomer)
 - hash_float accepts any type (casts to str)
 - shipment_id generated and stored as string (avoids psycopg2 UUID adapter issue)
 - insert_shipments_and_events inserts into the correct columns and str() casts shipment_id
"""
import argparse
import os
import sys
import json
import hashlib
from datetime import datetime, timedelta, timezone
import random
import pandas as pd
import psycopg2
import psycopg2.extras as pge

# ------------------------- Helpers -------------------------

def env(name, default=None, cast=str):
    val = os.getenv(name, default)
    if val is None:
        return None
    try:
        return cast(val)
    except Exception:
        return val

def hash_float(s) -> float:
    """Deterministic 0..1 float based on string hash; accepts any type."""
    s_str = str(s)
    h = hashlib.sha256(s_str.encode("utf-8")).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF

def pick_weight_for_reference(ref: str) -> float:
    r = hash_float(ref)
    if r < 0.60:
        return 0.2 + 2.8 * r / 0.60
    elif r < 0.90:
        return 3.0 + 4.0 * (r - 0.60) / 0.30
    else:
        return 7.0 + 1.0 * (r - 0.90) / 0.10

def assign_zone_for_customer(cust: str) -> str:
    r = hash_float(cust)
    if r < 0.25:
        return "Local"
    elif r < 0.65:
        return "Regional"
    elif r < 0.95:
        return "National"
    else:
        return "CrossBorder"

def zone_distance_km(zone: str) -> float:
    base = {"Local": 30, "Regional": 250, "National": 800, "CrossBorder": 1500}[zone]
    return max(5.0, random.gauss(base, base * 0.2))

def choose_origin(ordernum) -> str:
    hubs = ["WH1-Paris", "WH2-Lyon", "WH3-Lille", "WH4-Bordeaux"]
    idx = int(hash_float(ordernum) * len(hubs)) % len(hubs)
    return hubs[idx]

def choose_carrier_service(zone: str):
    carriers = {
        "Local":      [("Chrono", "SAME_DAY"), ("Chrono", "24H"), ("GLS", "24H")],
        "Regional":   [("DHL", "24H"), ("GLS", "24H"), ("Geodis", "48H")],
        "National":   [("DHL", "48H"), ("Geodis", "48H"), ("GLS", "72H")],
        "CrossBorder":[("DHL", "ECONOMY"), ("DHL", "48H"), ("UPS", "ECONOMY")]
    }
    return random.choice(carriers[zone])

def profiles_catalog():
    return [
        ("Chrono", "SAME_DAY", 35, 12, 1.0, 0.06, 0.90, 0.12, {"PREP_DELAY":0.30,"LAST_MILE":0.50,"ADDRESS":0.15,"WEATHER":0.05}),
        ("Chrono", "24H",      45, 24, 2.0, 0.05, 0.75, 0.10, {"PREP_DELAY":0.25,"LAST_MILE":0.45,"ADDRESS":0.20,"WEATHER":0.10}),
        ("GLS",    "24H",      55, 24, 2.5, 0.07, 0.70, 0.09, {"HUB_CONGESTION":0.35,"LAST_MILE":0.40,"ADDRESS":0.15,"WEATHER":0.10}),
        ("DHL",    "24H",      60, 24, 2.5, 0.06, 0.85, 0.11, {"HUB_CONGESTION":0.30,"LINEHAUL":0.30,"LAST_MILE":0.25,"WEATHER":0.15}),
        ("Geodis", "48H",      65, 48, 4.0, 0.08, 0.65, 0.08, {"HUB_CONGESTION":0.40,"LINEHAUL":0.30,"WEATHER":0.30}),
        ("DHL",    "48H",      70, 48, 4.0, 0.07, 0.90, 0.10, {"HUB_CONGESTION":0.35,"LINEHAUL":0.35,"WEATHER":0.30}),
        ("GLS",    "72H",      60, 72, 6.0, 0.09, 0.60, 0.07, {"HUB_CONGESTION":0.35,"LINEHAUL":0.25,"LAST_MILE":0.25,"WEATHER":0.15}),
        ("DHL",    "ECONOMY",  65, 96, 8.0, 0.10, 0.80, 0.06, {"LINEHAUL":0.40,"HUB_CONGESTION":0.30,"WEATHER":0.30}),
        ("UPS",    "ECONOMY",  65, 96, 8.0, 0.10, 0.85, 0.06, {"LINEHAUL":0.45,"HUB_CONGESTION":0.25,"WEATHER":0.30}),
    ]

# ------------------------- DB -------------------------

def connect_db():
    # DSN from your current script (adjust as needed)
    dsn = "host={h} port={p} dbname={d} user={u} password={pw}".format(
        h=env("PGHOST","localhost"),
        p=env("PGPORT","5432"),
        d=env("PGDATABASE","logiops"),
        u=env("PGUSER","postgres"),
        pw=env("PGPASSWORD","313055")
    )
    return psycopg2.connect(dsn)

def create_tables(conn, schema="public"):
    cur = conn.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.carrier_profiles(
        carrier         TEXT NOT NULL,
        service_level   TEXT NOT NULL,
        base_speed_kmph NUMERIC,
        sla_hours       INT,
        eta_noise_hours NUMERIC,
        exception_rate  NUMERIC,
        base_rate_per_km NUMERIC,
        surcharge_per_kg NUMERIC,
        exception_mix   JSONB,
        PRIMARY KEY (carrier, service_level)
    );
    """)
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.shipments(
        shipment_id     UUID PRIMARY KEY,
        ordernumber     TEXT NOT NULL,
        codcustomer     TEXT,
        total_units     NUMERIC,
        n_lines         INT,
        carrier         TEXT,
        service_level   TEXT,
        origin          TEXT,
        destination_zone TEXT,
        distance_km     NUMERIC,
        weight_kg       NUMERIC,
        volume_m3       NUMERIC,
        ready_to_ship   TIMESTAMPTZ,
        ship_datetime   TIMESTAMPTZ,
        eta_datetime    TIMESTAMPTZ,
        delivery_datetime TIMESTAMPTZ,
        status          TEXT,
        cost_estimated  NUMERIC,
        FOREIGN KEY (carrier, service_level) REFERENCES {schema}.carrier_profiles(carrier, service_level)
    );
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_shipments_ordernumber ON {schema}.shipments(ordernumber);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_shipments_carrier    ON {schema}.shipments(carrier, service_level);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_shipments_dates      ON {schema}.shipments(ship_datetime, eta_datetime, delivery_datetime);")

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {schema}.shipment_events(
        event_id        BIGSERIAL PRIMARY KEY,
        shipment_id     UUID REFERENCES {schema}.shipments(shipment_id) ON DELETE CASCADE,
        event_time      TIMESTAMPTZ,
        event_type      TEXT,
        location        TEXT,
        reason_code     TEXT,
        reason_label    TEXT
    );
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_events_shipment ON {schema}.shipment_events(shipment_id, event_time);")
    conn.commit()
    cur.close()

def upsert_carrier_profiles(conn, schema="public"):
    cur = conn.cursor()
    for (carrier, service, speed, sla, noise, ex_rate, rate_km, s_perkg, mix) in profiles_catalog():
        cur.execute(f"""
        INSERT INTO {schema}.carrier_profiles(carrier, service_level, base_speed_kmph, sla_hours, eta_noise_hours,
                 exception_rate, base_rate_per_km, surcharge_per_kg, exception_mix)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (carrier, service_level) DO UPDATE SET
            base_speed_kmph = EXCLUDED.base_speed_kmph,
            sla_hours       = EXCLUDED.sla_hours,
            eta_noise_hours = EXCLUDED.eta_noise_hours,
            exception_rate  = EXCLUDED.exception_rate,
            base_rate_per_km= EXCLUDED.base_rate_per_km,
            surcharge_per_kg= EXCLUDED.surcharge_per_kg,
            exception_mix   = EXCLUDED.exception_mix;
        """, (carrier, service, speed, sla, noise, ex_rate, rate_km, s_perkg, json.dumps(mix)))
    conn.commit()
    cur.close()

# ------------------------- Data load -------------------------

def load_orders_from_db(conn, schema="public", table="clean_customer_orders") -> pd.DataFrame:
    sql = f"""
    SELECT ordernumber, codcustomer, reference, quantity_units, creationdate
    FROM {schema}.{table}
    WHERE creationdate IS NOT NULL
    """
    df = pd.read_sql(sql, conn)
    return df

def load_orders_from_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    cols = {c.lower():c for c in df.columns}
    def find(name):
        for k in cols:
            if k == name: return cols[k]
        raise KeyError(f"Missing column '{name}' in CSV")
    df = df.rename(columns={
        find("ordernumber"): "ordernumber",
        find("codcustomer"): "codcustomer",
        find("reference"): "reference",
        find("quantity_units"): "quantity_units",
        find("creationdate"): "creationdate"
    })
    return df

def aggregate_orders(df: pd.DataFrame):
    df = df.copy()
    df["creationdate"] = pd.to_datetime(df["creationdate"], errors="coerce", utc=True)
    df["reference"] = df["reference"].astype(str).str.upper()
    df["codcustomer"] = df["codcustomer"].astype(str)
    df["ordernumber"] = df["ordernumber"].astype(str)  # important
    df["quantity_units"] = pd.to_numeric(df["quantity_units"], errors="coerce").fillna(0)

    df["unit_weight_kg"] = df["reference"].map(lambda r: pick_weight_for_reference(r))
    df["line_weight_kg"] = df["quantity_units"] * df["unit_weight_kg"]

    order_lines = df[["ordernumber","codcustomer","reference","quantity_units","creationdate","unit_weight_kg","line_weight_kg"]].copy()

    orders = df.groupby(["ordernumber","codcustomer"], as_index=False).agg(
        total_units=("quantity_units","sum"),
        n_lines=("reference","nunique"),
        creation_min=("creationdate","min"),
        creation_max=("creationdate","max"),
        weight_kg=("line_weight_kg","sum")
    )
    orders["volume_m3"] = orders["weight_kg"].apply(lambda w: max(0.005, (w/250.0) * (0.8 + 0.4*random.random())))
    return orders, order_lines

# ------------------------- Simulation -------------------------

def minutes_round_to_business(dt: datetime) -> datetime:
    if dt.hour < 8:
        return dt.replace(hour=8, minute=0, second=0, microsecond=0)
    if dt.hour >= 18:
        return (dt + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
    return dt.replace(second=0, microsecond=0)

def simulate_shipment_row(order, profiles_map):
    ordernumber = str(order["ordernumber"])  # ensure string
    codcustomer = str(order["codcustomer"])
    total_units = float(order["total_units"])
    n_lines     = int(order["n_lines"])
    creation    = order["creation_max"]
    ready_to_ship = minutes_round_to_business(creation + timedelta(hours=random.uniform(2, 10)))

    origin = choose_origin(ordernumber)
    zone   = assign_zone_for_customer(codcustomer)
    distance = zone_distance_km(zone)

    carrier, service = choose_carrier_service(zone)
    prof = profiles_map[(carrier, service)]

    base_speed = float(prof["base_speed_kmph"])
    sla_hours  = int(prof["sla_hours"])
    eta_noise  = float(prof["eta_noise_hours"])
    exception_rate = float(prof["exception_rate"])
    base_rate_km = float(prof["base_rate_per_km"])
    surcharge_per_kg = float(prof["surcharge_per_kg"])
    exception_mix = prof["exception_mix"]

    ship_dt = minutes_round_to_business(ready_to_ship + timedelta(minutes=random.uniform(10, 240)))
    travel_h = distance / max(25.0, base_speed)
    dwell_h  = random.uniform(0.5, 3.0 if sla_hours<=24 else 6.0 if sla_hours<=48 else 12.0)
    eta_promised = ship_dt + timedelta(hours=sla_hours) + timedelta(hours=random.uniform(-eta_noise, eta_noise))

    has_exception = random.random() < exception_rate
    reason_code = None
    extra_delay_h = 0.0
    if has_exception:
        R = random.random()
        acc = 0.0
        for k,v in exception_mix.items():
            acc += float(v)
            if R <= acc:
                reason_code = k
                break
        delays = {
            "PREP_DELAY":     random.uniform(1.0, 8.0),
            "HUB_CONGESTION": random.uniform(2.0, 10.0),
            "LINEHAUL":       random.uniform(4.0, 24.0),
            "LAST_MILE":      random.uniform(1.0, 6.0),
            "ADDRESS":        random.uniform(6.0, 24.0),
            "WEATHER":        random.uniform(6.0, 36.0)
        }
        extra_delay_h = delays.get(reason_code, random.uniform(2.0, 12.0))

    actual_h = travel_h + dwell_h + random.uniform(-0.5, 2.0) + extra_delay_h
    delivery_dt = ship_dt + timedelta(hours=max(0.5, actual_h))

    weight_kg = float(order["weight_kg"])
    volume_m3 = float(order["volume_m3"])
    cost_est  = base_rate_km * distance + surcharge_per_kg * weight_kg

    status = "DELIVERED" if delivery_dt <= datetime.now(timezone.utc) + timedelta(days=365*10) else "IN_TRANSIT"

    row = {
        "ordernumber": ordernumber,
        "codcustomer": codcustomer,
        "total_units": total_units,
        "n_lines": n_lines,
        "carrier": carrier,
        "service_level": service,
        "origin": origin,
        "destination_zone": zone,
        "distance_km": round(distance, 1),
        "weight_kg": round(weight_kg, 3),
        "volume_m3": round(volume_m3, 4),
        "ready_to_ship": ship_dt - timedelta(hours=random.uniform(0.2, 2.0)),  # slight gap before ship
        "ship_datetime": ship_dt,
        "eta_datetime": eta_promised,
        "delivery_datetime": delivery_dt,
        "status": status,
        "cost_estimated": round(cost_est, 2),
        "exception_code": reason_code
    }
    return row

def simulate_events_for_shipment(shipment_row):
    sid = shipment_row["shipment_id"]
    origin = shipment_row["origin"]
    zone   = shipment_row["destination_zone"]
    ship_dt= shipment_row["ship_datetime"]
    deliv  = shipment_row["delivery_datetime"]
    carrier= shipment_row["carrier"]

    total_h = (deliv - ship_dt).total_seconds()/3600.0
    total_h = max(total_h, 2.0)
    hub_in   = ship_dt + timedelta(hours=total_h*0.25)
    hub_out  = ship_dt + timedelta(hours=total_h*0.50)
    ofd      = ship_dt + timedelta(hours=total_h*0.80)

    events = [
        ("pickup", ship_dt, origin, None, None),
        ("hub_in", hub_in, f"{carrier}-hub", None, None),
        ("hub_out", hub_out, f"{carrier}-hub", None, None),
        ("out_for_delivery", ofd, zone, None, None),
        ("delivered", deliv, zone, None, None),
    ]

    if shipment_row.get("exception_code"):
        rc = shipment_row["exception_code"]
        if rc in ("HUB_CONGESTION","LINEHAUL","WEATHER"):
            et = hub_out + timedelta(hours=1)
            loc = f"{carrier}-hub"
        elif rc in ("LAST_MILE","ADDRESS"):
            et = ofd + timedelta(hours=0.5)
            loc = zone
        else:
            et = ship_dt - timedelta(minutes=30)
            loc = origin
        events.insert(1, ("exception", et, loc, rc, rc.title().replace("_"," ")))

    out = []
    for etype, etime, loc, rcode, rlabel in events:
        out.append({
            "shipment_id": sid,
            "event_time": etime,
            "event_type": etype,
            "location": loc,
            "reason_code": rcode,
            "reason_label": rlabel
        })
    return out

# ------------------------- Load to DB -------------------------

def safe_overwrite(conn, schema):
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {schema}.shipment_events;")
    cur.execute(f"TRUNCATE TABLE {schema}.shipments;")
    conn.commit()
    cur.close()

def insert_shipments_and_events(conn, schema, shipments_rows, events_rows):
    # Ensure shipment_id are strings (not uuid.UUID objects)
    prepared_shipments = [
        (
            str(r["shipment_id"]),
            r["ordernumber"],
            r.get("codcustomer"),
            r["total_units"],
            r["n_lines"],
            r["carrier"],
            r["service_level"],
            r["origin"],
            r["destination_zone"],
            r["distance_km"],
            r["weight_kg"],
            r["volume_m3"],
            r["ready_to_ship"],
            r["ship_datetime"],
            r["eta_datetime"],
            r["delivery_datetime"],
            r["status"],
            r["cost_estimated"],
        )
        for r in shipments_rows
    ]

    prepared_events = [
        (
            str(e["shipment_id"]),
            e["event_time"],
            e["event_type"],
            e["location"],
            e["reason_code"],
            e["reason_label"]
        )
        for e in events_rows
    ]

    with conn.cursor() as cur:
        pge.execute_values(cur, f"""
            INSERT INTO {schema}.shipments(
                shipment_id, ordernumber, codcustomer, total_units, n_lines, carrier, service_level,
                origin, destination_zone, distance_km, weight_kg, volume_m3,
                ready_to_ship, ship_datetime, eta_datetime, delivery_datetime, status, cost_estimated
            )
            VALUES %s
        """, prepared_shipments)

        if prepared_events:
            pge.execute_values(cur, f"""
                INSERT INTO {schema}.shipment_events(
                    shipment_id, event_time, event_type, location, reason_code, reason_label
                )
                VALUES %s
            """, prepared_events)

    conn.commit()

# ------------------------- Main -------------------------

def main():
    ap = argparse.ArgumentParser(description="Simulate transport tables from orders and load into PostgreSQL")
    ap.add_argument("--schema", default="public", help="Target schema (default: public)")
    ap.add_argument("--orders-source", choices=["db","csv"], default="db", help="Read orders from 'db' or 'csv'")
    ap.add_argument("--orders-csv", help="Path to clean_customer_orders.csv if --orders-source=csv")
    ap.add_argument("--overwrite", action="store_true", help="Truncate shipments & shipment_events before load")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    ap.add_argument("--batch-size", type=int, default=20000, help="Batch insert size (rows)")
    args = ap.parse_args()

    random.seed(args.seed)

    conn = connect_db()
    create_tables(conn, args.schema)
    upsert_carrier_profiles(conn, args.schema)
    if args.overwrite:
        safe_overwrite(conn, args.schema)

    if args.orders_source == "db":
        orders_lines = load_orders_from_db(conn, args.schema)
    else:
        if not args.orders_csv or not os.path.exists(args.orders_csv):
            print("When --orders-source=csv, provide --orders-csv path.", file=sys.stderr)
            sys.exit(2)
        orders_lines = load_orders_from_csv(args.orders_csv)

    if orders_lines.empty:
        print("No orders found. Aborting.", file=sys.stderr)
        sys.exit(1)

    orders_hdr, order_lines = aggregate_orders(orders_lines)

    with conn.cursor(cursor_factory=pge.RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM {args.schema}.carrier_profiles;")
        profs = cur.fetchall()
    profiles_map = {(p["carrier"], p["service_level"]): p for p in profs}

    from uuid import uuid4
    shipments_rows, events_rows = [], []
    for _, od in orders_hdr.iterrows():
        row = simulate_shipment_row(od, profiles_map)
        row["shipment_id"] = str(uuid4())  # generate as string
        shipments_rows.append(row)
        # add the shipment_id string into events
        tmp_events = simulate_events_for_shipment(row)
        events_rows.extend(tmp_events)

    total = len(shipments_rows)
    bs = max(1000, args.batch_size)
    for i in range(0, total, bs):
        batch_ship = shipments_rows[i:i+bs]
        batch_ids = {r["shipment_id"] for r in batch_ship}
        batch_ev   = [e for e in events_rows if e["shipment_id"] in batch_ids]
        insert_shipments_and_events(conn, args.schema, batch_ship, batch_ev)
        print(f"Inserted {min(i+bs,total)}/{total} shipments...")

    conn.close()
    print("Done. Tables populated: carrier_profiles, shipments, shipment_events.")

if __name__ == "__main__":
    main()
