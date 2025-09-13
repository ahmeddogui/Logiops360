from __future__ import annotations
from flask import jsonify, request
from pathlib import Path
import pandas as pd
from . import bp
from .shared import join_unified_locations, capacity_map, make_location_agg, velocity_proxy, infer_zone_from_location

OUT_DIR = Path(__file__).resolve().parents[1] / "models" / "storage"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CSV_PLAN = OUT_DIR / "slotting_action_plan.csv"

@bp.get("/slotting/plan")
def slotting_plan():
    top_k_in = int(request.args.get("top_k_in", 50))
    top_k_out = int(request.args.get("top_k_out", 50))

    df = join_unified_locations()
    cap = capacity_map(df)
    loc = make_location_agg(df, cap)
    vel = velocity_proxy(df)

    # zones (A=fast via 1ère lettre)
    loc["zone"] = loc["location"].map(infer_zone_from_location)
    df_zone = df.merge(loc[["location","zone"]], on="location", how="left")

    # SKUs dans A (fast)
    sku_in_fast = (df_zone[df_zone["zone"]=="A"]
                   .groupby("referenceproduit")["on_hand"].sum().reset_index())
    sku_in_fast["in_fast"] = True

    v = vel.merge(sku_in_fast[["referenceproduit","in_fast"]],
                  on="referenceproduit", how="left")
    # évite le downcasting silencieux (et le FutureWarning)
    v["in_fast"] = v["in_fast"].fillna(False).astype(bool)

    out_candidates = v[v["in_fast"]].sort_values("vel_score", ascending=True).head(top_k_out)
    in_candidates  = v[~v["in_fast"]].sort_values("vel_score", ascending=False).head(top_k_in)

    fast_cap  = float(loc.loc[loc["zone"]=="A","loc_capacity"].sum())
    fast_used = float(loc.loc[loc["zone"]=="A","on_hand"].sum())
    fast_free = max(0.0, fast_cap - fast_used)

    plan_rows = []

    # OUT
    for _, r in out_candidates.iterrows():
        move_qty = float(df_zone[(df_zone["referenceproduit"]==r["referenceproduit"]) & (df_zone["zone"]=="A")]["on_hand"].sum())
        if move_qty <= 0: 
            continue
        plan_rows.append({
            "referenceproduit": r["referenceproduit"],
            "from_zone": "A",
            "to_zone": "B/C/D",
            "move_qty": int(round(move_qty)),
            "reason": "Faible vélocité en fast"
        })
        fast_used -= move_qty
        fast_free = max(0.0, fast_cap - fast_used)

    # IN
    for _, r in in_candidates.iterrows():
        sku_qty_total = float(df[df["referenceproduit"]==r["referenceproduit"]]["on_hand"].sum())
        if sku_qty_total <= 0 or fast_free <= 0:
            continue
        move_qty = int(min(sku_qty_total, fast_free))
        if move_qty <= 0:
            continue
        plan_rows.append({
            "referenceproduit": r["referenceproduit"],
            "from_zone": "B/C/D",
            "to_zone": "A",
            "move_qty": int(move_qty),
            "reason": "Haute vélocité hors fast"
        })
        fast_used += move_qty
        fast_free = max(0.0, fast_cap - fast_used)

    pd.DataFrame(plan_rows, columns=["referenceproduit","from_zone","to_zone","move_qty","reason"]).to_csv(CSV_PLAN, index=False, encoding="utf-8")

    return jsonify({
        "summary": {
            "fast_capacity": fast_cap,
            "fast_used_before": float(loc.loc[loc["zone"]=="A","on_hand"].sum()),
            "fast_used_after": fast_used,
            "moves": len(plan_rows)
        },
        "sample": plan_rows[:20],
        "csv_path": str(CSV_PLAN)
    })
