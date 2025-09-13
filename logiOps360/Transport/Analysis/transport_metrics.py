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
from scipy.stats import kruskal, mannwhitneyu
from statsmodels.stats.proportion import proportions_ztest
from statsmodels.stats.multitest import multipletests

OUTDIR_DEFAULT = "outputs"

def export_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def try_col(df: pd.DataFrame, choices):
    return next((c for c in choices if c in df.columns), None)

def main(outdir: str = OUTDIR_DEFAULT):
    outdir = Path(outdir)
    conn = connect_db()
    try:
        # --- Shipments ---
        ship = pd.read_sql("SELECT * FROM shipments;", conn)

        col_eta  = try_col(ship, ["eta_planned","eta","eta_plan"])
        col_del  = try_col(ship, ["t_delivered","delivered_at","delivery_real","delivery_time"])
        col_pick = try_col(ship, ["t_pickup","pickup_time","picked_at"])
        col_dist = try_col(ship, ["distance_km","distance"])
        col_sv   = try_col(ship, ["service_level","service"])
        col_car  = try_col(ship, ["carrier_id","carrier","transporteur"])
        sid      = try_col(ship, ["shipment_id","id"])

        if not (col_eta and col_del and sid):
            raise RuntimeError("Colonnes critiques manquantes dans 'shipments' (eta_planned / t_delivered / shipment_id).")

        for c in [col_eta, col_del, col_pick]:
            if c and not np.issubdtype(pd.Series(ship[c]).dtype, np.datetime64):
                ship[c] = pd.to_datetime(ship[c], errors="coerce")

        df = ship[[sid, col_eta, col_del, col_pick, col_dist, col_sv, col_car]].copy()
        df = df.rename(columns={
            sid:"shipment_id", col_eta:"eta_planned", col_del:"t_delivered",
            col_pick:"t_pickup", col_dist:"distance_km", col_sv:"service_level", col_car:"carrier"
        })
        df["on_time"] = (df["t_delivered"].notna()) & (df["eta_planned"].notna()) & (df["t_delivered"] <= df["eta_planned"])
        df["eta_error_min"] = (df["t_delivered"] - df["eta_planned"]).dt.total_seconds()/60.0
        if "t_pickup" in df:
            df["transit_hours"] = (df["t_delivered"] - df["t_pickup"]).dt.total_seconds()/3600.0

        # --- Events (optionnels) : nb_hubs, exceptions ---
        try:
            ev = pd.read_sql("SELECT * FROM shipment_events;", conn)
            col_sid = try_col(ev, ["shipment_id"])
            col_etp = try_col(ev, ["event_type"])
            col_tim = try_col(ev, ["event_time","ts"])
            if col_tim and not np.issubdtype(pd.Series(ev[col_tim]).dtype, np.datetime64):
                ev[col_tim] = pd.to_datetime(ev[col_tim], errors="coerce")
            nb_hubs = (ev.query(f"{col_etp}=='hub_in'")
                        .groupby(col_sid)[col_tim].nunique()
                        .rename("nb_hubs"))
            exc_cnt = (ev[ev[col_etp].str.startswith("exception", na=False)]
                        .groupby(col_sid)[col_tim].size()
                        .rename("n_exceptions"))
            df = df.merge(nb_hubs, left_on="shipment_id", right_index=True, how="left")
            df = df.merge(exc_cnt, left_on="shipment_id", right_index=True, how="left")
        except Exception:
            pass

        export_csv(df, outdir / "transport_kpi.csv")

        # --- H1 : OTD 24h vs 48h (test de proportion) ---
        out_rows = []
        sv_24 = df[df["service_level"].astype(str).str.contains("24", case=False, na=False)]
        sv_48 = df[df["service_level"].astype(str).str.contains("48", case=False, na=False)]
        if len(sv_24)>0 and len(sv_48)>0:
            count = np.array([sv_24["on_time"].sum(), sv_48["on_time"].sum()])
            nobs  = np.array([sv_24["on_time"].count(), sv_48["on_time"].count()])
            zstat, pval = proportions_ztest(count, nobs, alternative="larger")
            out_rows.append({"test":"H1_OTD_24_vs_48", "z":float(zstat), "p":float(pval),
                             "p24":float(count[0]/nobs[0]), "p48":float(count[1]/nobs[1]),
                             "n24":int(nobs[0]), "n48":int(nobs[1])})
        else:
            out_rows.append({"test":"H1_OTD_24_vs_48", "note":"niveaux 24h/48h insuffisants ou absents"})

        # --- H2 : ETA error par transporteur (Kruskal + post-hoc) ---
        eta_by_carrier = df.dropna(subset=["eta_error_min","carrier"]).groupby("carrier")["eta_error_min"].apply(list)
        if len(eta_by_carrier) >= 2:
            kw_stat, kw_p = kruskal(*eta_by_carrier.tolist())
            out_rows.append({"test":"H2_KW_eta_error_by_carrier", "kw_stat":float(kw_stat), "kw_p":float(kw_p)})
            pairs = list(itertools.combinations(eta_by_carrier.index, 2))
            ph = []
            for a,b in pairs:
                stat, p = mannwhitneyu(eta_by_carrier[a], eta_by_carrier[b], alternative="two-sided")
                ph.append({"a":a,"b":b,"mw_stat":float(stat),"p_raw":float(p)})
            ph_df = pd.DataFrame(ph)
            if not ph_df.empty:
                ph_df["p_adj"] = multipletests(ph_df["p_raw"], method="fdr_bh")[1]
            export_csv(ph_df, outdir / "transport_h2_posthoc.csv")
        else:
            out_rows.append({"test":"H2_KW_eta_error_by_carrier", "note":"carriers insuffisants"})

        export_csv(pd.DataFrame(out_rows), outdir / "transport_h1_h2.csv")
        print("✅ Transport — résultats écrits dans", outdir)

    finally:
        try:
            if hasattr(conn, "dispose"): conn.dispose()
            elif hasattr(conn, "close"): conn.close()
        except Exception: pass

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv)>1 else OUTDIR_DEFAULT)
