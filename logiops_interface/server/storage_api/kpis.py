from __future__ import annotations
from flask import jsonify
from . import bp
from .shared import join_unified_locations, capacity_map, make_location_agg

@bp.get("/kpis")
def storage_kpis():
    df = join_unified_locations()
    cap = capacity_map(df)
    loc = make_location_agg(df, cap)

    on_hand_total = float(loc["on_hand"].sum())
    capacity_total = float(loc["loc_capacity"].sum()) or 1.0
    occupancy_rate = on_hand_total / capacity_total

    active_locations = int((loc["on_hand"] > 0).sum())
    distinct_skus = int(df["referenceproduit"].nunique())
    saturated_pct = float(((loc["occ_ratio"] >= 0.90).sum()) / len(loc)) if len(loc) else 0.0

    return jsonify({
        "occupancy_rate": round(occupancy_rate, 4),
        "active_locations": active_locations,
        "distinct_skus": distinct_skus,
        "saturated_locations_pct": round(saturated_pct, 4),
        "totals": {"on_hand": on_hand_total, "capacity": capacity_total}
    })
