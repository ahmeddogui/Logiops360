#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, importlib
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from utils.db_utils import connect_db

import os, itertools
import pandas as pd
import numpy as np
from scipy.stats import kruskal, spearmanr, mannwhitneyu
from statsmodels.stats.multitest import multipletests

OUTDIR_DEFAULT = "outputs"

def export_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def _norm(df: pd.DataFrame, ref_candidates, qty_candidates, stype: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["location","reference","quantity","storage_type"])
    d = df.copy()
    ref_col = next((c for c in ref_candidates if c in d.columns), None)
    qty_col = next((c for c in qty_candidates if c in d.columns), None)
    if ref_col is None: raise KeyError("Colonne référence introuvable")
    if qty_col is None: raise KeyError("Colonne quantité introuvable")
    d = d.rename(columns={ref_col:"reference", qty_col:"quantity"})
    out = d[["location","reference","quantity"]].copy()
    out["storage_type"] = stype
    return out

def main(outdir: str = OUTDIR_DEFAULT):
    outdir = Path(outdir)
    conn = connect_db()
    try:
        # Chargement tables
        try: cb = pd.read_sql("SELECT * FROM clean_class_based_storage;", conn)
        except Exception: cb = None
        try: de = pd.read_sql("SELECT * FROM clean_dedicated_storage;", conn)
        except Exception: de = None
        try: ra = pd.read_sql("SELECT * FROM clean_random_storage;", conn)
        except Exception: ra = None
        try: hy = pd.read_sql("SELECT * FROM clean_hybrid_storage;", conn)
        except Exception: hy = None

        sl  = pd.read_sql("SELECT * FROM clean_storage_location;", conn)
        sp  = pd.read_sql("SELECT * FROM clean_support_points;", conn)

        cb_n = _norm(cb, ["referenceproduit","reference"], ["quantity","quantity_units","qty"], "class_based")
        de_n = _norm(de, ["referenceproduit","reference"], ["quantity","quantity_units","qty"], "dedicated")
        ra_n = _norm(ra, ["referenceproduit","reference"], ["quantity","quantity_units","qty"], "random")
        # hybrid => matérielle
        hy_n = _norm(hy, ["material","reference"], ["quantity","quantity_units","qty"], "hybrid")

        storage = pd.concat([cb_n, de_n, ra_n, hy_n], ignore_index=True)
        storage["quantity"] = storage["quantity"].fillna(0)

        coords = sl[["location","support_label","x","y","z"]].merge(
            sp.rename(columns={"label":"support_label","x_coord":"sx","y_coord":"sy","z_coord":"sz"}),
            on="support_label", how="left"
        )
        storage = storage.merge(coords, on="location", how="left").dropna(subset=["x","y","z","sx","sy","sz"])

        storage["distance_to_support"] = np.sqrt(
            (storage["x"]-storage["sx"])**2 + (storage["y"]-storage["sy"])**2 + (storage["z"]-storage["sz"])**2
        )

        # --- H3 : distances par type + tests ---
        summary = (storage
            .groupby("storage_type")
            .apply(lambda g: pd.Series({
                "n": len(g),
                "qty_sum": float(g["quantity"].sum()),
                "distance_mean_w": float(np.average(g["distance_to_support"], weights=np.maximum(g["quantity"],1))),
                "distance_median_rep": float(g.loc[g.index.repeat(np.maximum(g["quantity"].astype(int),1)),"distance_to_support"].median())
            }))
            .reset_index()
        )
        export_csv(summary, outdir / "stockage_dist_by_type.csv")

        groups = [g["distance_to_support"].values for _, g in storage.groupby("storage_type")]
        kw_stat, kw_p = kruskal(*groups)
        pairs = list(itertools.combinations(storage["storage_type"].dropna().unique(), 2))
        ph = []
        for a,b in pairs:
            ga = storage.loc[storage["storage_type"]==a, "distance_to_support"]
            gb = storage.loc[storage["storage_type"]==b, "distance_to_support"]
            stat, p = mannwhitneyu(ga, gb, alternative="two-sided")
            ph.append({"type_a":a,"type_b":b,"mw_stat":stat,"p_raw":p})
        ph_df = pd.DataFrame(ph)
        if not ph_df.empty:
            ph_df["p_adj"] = multipletests(ph_df["p_raw"], method="fdr_bh")[1]
        ph_df["kw_stat_global"] = kw_stat
        ph_df["kw_p_global"] = kw_p
        export_csv(ph_df, outdir / "stockage_kw_posthoc.csv")

        # --- H4 : Spearman (dispersion vs distance moyenne) ---
        disp = storage.groupby("reference")["location"].nunique().rename("n_locations")
        avgd = storage.groupby("reference").apply(
            lambda g: np.average(g["distance_to_support"], weights=np.maximum(g["quantity"],1))
        ).rename("avg_distance")
        pdist = pd.concat([disp, avgd], axis=1).dropna()
        rho, pval = spearmanr(pdist["n_locations"], pdist["avg_distance"])
        pdist.assign(spearman_rho=rho, spearman_p=pval).to_csv(outdir / "stockage_dispersion_corr.csv", index=False)

        synth = pd.DataFrame([{
            "H3_Kruskal_stat": kw_stat, "H3_Kruskal_p": kw_p,
            "H4_Spearman_rho": rho, "H4_Spearman_p": pval
        }])
        export_csv(synth, outdir / "stockage_hypotheses.csv")
        print("✅ Stockage — résultats écrits dans", outdir)
        print(synth.to_string(index=False))

    finally:
        try:
            if hasattr(conn, "dispose"): conn.dispose()
            elif hasattr(conn, "close"): conn.close()
        except Exception: pass

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv)>1 else OUTDIR_DEFAULT)
