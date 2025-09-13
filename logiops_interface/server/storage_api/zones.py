from __future__ import annotations
from flask import jsonify
from . import bp
from .shared import join_unified_locations, capacity_map, make_location_agg, make_zone_agg

@bp.get("/zones/occupancy")
def zones_occupancy():
    df = join_unified_locations()
    cap = capacity_map(df)
    loc = make_location_agg(df, cap)
    z = make_zone_agg(loc)
    items = [{
        "zone": str(r["zone"]),
        "n_locations": int(r["n_locations"]),
        "on_hand": float(r["on_hand"]),
        "capacity": float(r["capacity"]),
        "occupancy_pct": round(float(r["occupancy_pct"]), 4),
        "status": r["status"],
    } for _, r in z.sort_values("zone").iterrows()]
    return jsonify({"items": items})
