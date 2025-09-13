from __future__ import annotations
from flask import jsonify
import pandas as pd
from . import bp
from .shared import join_unified_locations, capacity_map, make_location_agg, load_supports

@bp.get("/map")
def warehouse_map():
    df = join_unified_locations()
    sup = load_supports()
    cap = capacity_map(df)
    loc = make_location_agg(df, cap)

    sup_agg = loc.groupby("support_label", dropna=False).agg(
        n_locations=("location","nunique"),
        on_hand=("on_hand","sum"),
        capacity=("loc_capacity","sum"),
    ).reset_index()
    sup_agg["occupancy_pct"] = (sup_agg["on_hand"]/sup_agg["capacity"]).replace([float("inf"),-float("inf")],0).fillna(0.0)
    sup_agg = sup_agg.merge(sup, on="support_label", how="left")

    items = []
    for _, r in sup_agg.iterrows():
        items.append({
            "support_label": None if pd.isna(r["support_label"]) else str(r["support_label"]),
            "lat": None if pd.isna(r.get("lat")) else float(r["lat"]),
            "lon": None if pd.isna(r.get("lon")) else float(r["lon"]),
            "n_locations": int(r["n_locations"]),
            "on_hand": float(r["on_hand"]),
            "capacity": float(r["capacity"]),
            "occupancy_pct": round(float(r["occupancy_pct"]), 4),
        })
    return jsonify({"items": items})
