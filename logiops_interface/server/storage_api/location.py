from flask import jsonify
from . import bp
from .shared import join_unified_locations, capacity_map, make_location_agg

@bp.get("/location/<loc>")
def location_detail(loc):
    df = join_unified_locations()
    cap = capacity_map(df)
    loc_agg = make_location_agg(df, cap)
    row = loc_agg[loc_agg["location"] == loc]
    if row.empty:
        return jsonify({"error": f"location {loc} not found"}), 404

    r = row.iloc[0]
    details = {
        "location": loc,
        "on_hand": float(r["on_hand"]),
        "n_skus": int(r["n_skus"]),
        "capacity": float(r["loc_capacity"]),
        "occ_ratio": round(float(r["occ_ratio"]), 3),
    }

    # détail produits
    subset = df[df["location"] == loc].groupby("referenceproduit")["on_hand"].sum().reset_index()
    subset = subset.sort_values("on_hand", ascending=False)
    details["products"] = [
        {"reference": str(rr["referenceproduit"]), "qty": float(rr["on_hand"])}
        for _, rr in subset.iterrows()
    ]

    return jsonify(details)
