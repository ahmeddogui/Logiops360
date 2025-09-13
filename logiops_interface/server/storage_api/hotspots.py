from __future__ import annotations
from flask import jsonify, request
import numpy as np
from . import bp
from .shared import join_unified_locations, capacity_map, make_location_agg, velocity_proxy

@bp.get("/hotspots")
def hotspots():
    n_skus_max = int(request.args.get("nskus_max", 25))
    horizon_jours = int(request.args.get("h", 7))
    total_daily_demand = float(request.args.get("daily_demand", 1000))

    df = join_unified_locations()
    cap = capacity_map(df)
    loc = make_location_agg(df, cap)

    # 1) Surcharge / Trop de références
    mask_over = loc["occ_ratio"] > 1.0
    mask_too_many = loc["n_skus"] > n_skus_max
    surcharge = loc[mask_over | mask_too_many].copy()
    surcharge["reason"] = np.where(surcharge["occ_ratio"] > 1.0, "Surcharge", "Trop de références")

    # 2) Rupture prévue
    vel = velocity_proxy(df)
    weights = vel["vel_score"].clip(lower=1e-6)
    weights = weights / weights.sum()
    vel["forecast_daily"] = total_daily_demand * weights

    # sécurité : si df n’a pas on_hand, on calcule à la volée
    if "on_hand" in df.columns:
        sku_onhand = df.groupby("referenceproduit")["on_hand"].sum().reset_index()
    else:
        sku_onhand = vel[["referenceproduit"]].copy()
        sku_onhand["on_hand"] = 0.0

    risk = vel.merge(sku_onhand, on="referenceproduit", how="left", suffixes=("", "_sku"))
    # après merge, on peut avoir deux colonnes : vel["on_hand"] et sku_onhand["on_hand"]
    if "on_hand_sku" in risk.columns:
        risk = risk.rename(columns={"on_hand_sku": "stock_on_hand"})
    else:
        risk = risk.rename(columns={"on_hand": "stock_on_hand"})

    risk["doc_days"] = (risk["stock_on_hand"] / risk["forecast_daily"]).replace([np.inf, -np.inf], np.nan)
    rupture_skus = risk[risk["doc_days"] < horizon_jours].dropna(subset=["doc_days"])

    # localisation principale
    if "on_hand" in df.columns:
        sku_loc = df.groupby(["referenceproduit", "location"])["on_hand"].sum().reset_index()
    else:
        sku_loc = df[["referenceproduit", "location"]].copy()
        sku_loc["on_hand"] = 0

    sku_main = sku_loc.sort_values(["referenceproduit","on_hand"], ascending=[True, False]).drop_duplicates("referenceproduit")
    rupture = rupture_skus.merge(sku_main, on="referenceproduit", how="left")
    rupture["reason"] = "Rupture prévue"

    # 3) Mauvais slotting
    loc["zone"] = loc["location"].astype(str).str[0].str.upper()
    df_zone = df.merge(loc[["location","zone"]], on="location", how="left")
    vel_zone = vel.merge(df_zone[["referenceproduit","zone"]].drop_duplicates(), on="referenceproduit", how="left")

    low_in_fast = vel_zone[(vel_zone["vel_score"] < 0.3) & (vel_zone["zone"] == "A")]
    high_out_fast = vel_zone[(vel_zone["vel_score"] > 0.7) & (vel_zone["zone"] != "A")]

    mauvais = (
        [{"location": None, "referenceproduit": r["referenceproduit"], "reason": "Mauvais slotting (lent en A)"} for _, r in low_in_fast.iterrows()]
        +
        [{"location": None, "referenceproduit": r["referenceproduit"], "reason": "Mauvais slotting (rapide hors A)"} for _, r in high_out_fast.iterrows()]
    )

    # Compose la sortie
    items = []
    for _, r in surcharge.sort_values("occ_ratio", ascending=False).head(20).iterrows():
        items.append({
            "location": r["location"],
            "referenceproduit": None,
            "capacity_pct": int(round(float(r["occ_ratio"]) * 100)),
            "reason": r["reason"],
            "action": "Corriger"
        })
    for _, r in rupture.sort_values("doc_days").head(20).iterrows():
        items.append({
            "location": r.get("location"),
            "referenceproduit": r["referenceproduit"],
            "capacity_pct": 0,
            "reason": f"Rupture prévue (DoC={int(max(0, round(r['doc_days'])))} j)",
            "action": "Corriger"
        })
    for row in mauvais[:20]:
        items.append({
            "location": row["location"],
            "referenceproduit": row["referenceproduit"],
            "capacity_pct": None,
            "reason": row["reason"],
            "action": "Corriger"
        })

    return jsonify({"items": items})
