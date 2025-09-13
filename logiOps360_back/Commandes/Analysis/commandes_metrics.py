#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, importlib
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from utils.db_utils import connect_db

import os
import pandas as pd
import numpy as np
from scipy.stats import wilcoxon

OUTDIR_DEFAULT = "outputs"

def export_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def main(outdir: str = OUTDIR_DEFAULT):
    outdir = Path(outdir)
    conn = connect_db()  # Engine SQLAlchemy ou connexion DBAPI
    try:
        # --- H7 : RF vs baseline naïve S-1 ---
        sql_actual = """
        SELECT
          reference,
          date_trunc('week', creationdate)::date AS week_start,
          SUM(quantity_units)::float AS qty_actual
        FROM clean_customer_orders
        GROUP BY 1,2;
        """
        sql_fc = """
        SELECT
          reference,
          date_week::date AS week_start,
          qty_pred::float   AS qty_pred,
          COALESCE(model,'rf_weekly') AS model
        FROM fct_order_forecast;
        """
        actual = pd.read_sql(sql_actual, conn)
        fc = pd.read_sql(sql_fc, conn)

        rf = fc[fc["model"].astype(str).str.lower().str.contains("rf")].copy()
        if rf.empty and not fc.empty:
            rf = fc.copy()

        af = actual.merge(rf[["reference","week_start","qty_pred"]],
                          on=["reference","week_start"], how="inner")

        prev = actual.sort_values(["reference","week_start"]).copy()
        prev["qty_naive_pred"] = prev.groupby("reference")["qty_actual"].shift(1)
        af = af.merge(prev[["reference","week_start","qty_naive_pred"]],
                      on=["reference","week_start"], how="left").dropna()

        if not af.empty:
            af["ae_rf"] = (af["qty_pred"] - af["qty_actual"]).abs()
            af["ae_naive"] = (af["qty_naive_pred"] - af["qty_actual"]).abs()
            mae_rf = af["ae_rf"].mean()
            mae_nv = af["ae_naive"].mean()
            denom = af["qty_actual"].sum()
            wape_rf = af["ae_rf"].sum()/denom if denom else np.nan
            wape_nv = af["ae_naive"].sum()/denom if denom else np.nan
            improve_mae = (mae_nv - mae_rf)/mae_nv if mae_nv else np.nan
            improve_wape = (wape_nv - wape_rf)/wape_nv if wape_nv else np.nan
            try:
                w_stat, w_p = wilcoxon(af["ae_rf"], af["ae_naive"], zero_method="wilcox", alternative="less")
            except Exception:
                w_stat, w_p = np.nan, np.nan
            h7 = {
                "H7_N_obs": int(len(af)),
                "H7_MAE_RF": mae_rf, "H7_MAE_Naive": mae_nv,
                "H7_WAPE_RF": wape_rf, "H7_WAPE_Naive": wape_nv,
                "H7_Amélioration_MAE_%": None if pd.isna(improve_mae) else round(100*improve_mae,2),
                "H7_Amélioration_WAPE_%": None if pd.isna(improve_wape) else round(100*improve_wape,2),
                "H7_Wilcoxon_stat": w_stat, "H7_Wilcoxon_p": w_p,
                "H7_Décision": "RF < Naïf (significatif)" if (not pd.isna(w_p) and w_p < 0.05 and improve_mae > 0)
                               else ("Tendance RF < Naïf" if improve_mae > 0 else "Non concluant / Baseline ≥ RF")
            }
        else:
            h7 = {
                "H7_N_obs": 0,
                "H7_MAE_RF": np.nan, "H7_MAE_Naive": np.nan,
                "H7_WAPE_RF": np.nan, "H7_WAPE_Naive": np.nan,
                "H7_Amélioration_MAE_%": np.nan, "H7_Amélioration_WAPE_%": np.nan,
                "H7_Wilcoxon_stat": np.nan, "H7_Wilcoxon_p": np.nan,
                "H7_Décision": "Non évaluable (pas de recouvrement des semaines)"
            }

        export_csv(af, outdir / "commandes_weekly_join.csv")

        # --- H8 : Pareto top-20% ---
        sql_vol = """
        SELECT reference, SUM(quantity_units)::float AS qty
        FROM clean_customer_orders
        GROUP BY 1;
        """
        vol = pd.read_sql(sql_vol, conn).sort_values("qty", ascending=False).reset_index(drop=True)
        if not vol.empty:
            top_n = max(1, int(0.2*len(vol)))
            pareto_share = vol.iloc[:top_n]["qty"].sum()/vol["qty"].sum()
            h8 = {
                "H8_N_SKU": int(len(vol)),
                "H8_part_top20%": round(100*pareto_share,2),
                "H8_seuil_attendu_%": 80.0,
                "H8_Décision": "Validée" if 100*pareto_share >= 80 else "Non validée"
            }
        else:
            h8 = {
                "H8_N_SKU": 0,
                "H8_part_top20%": np.nan,
                "H8_seuil_attendu_%": 80.0,
                "H8_Décision": "Non évaluable (pas de volume)"
            }

        synth = pd.DataFrame([{**h7, **h8}])
        export_csv(synth, outdir / "commandes_hypotheses.csv")
        print("✅ Commandes — résultats écrits dans", outdir)
        print(synth.to_string(index=False))

    finally:
        try:
            if hasattr(conn, "dispose"): conn.dispose()
            elif hasattr(conn, "close"): conn.close()
        except Exception: pass

if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv)>1 else OUTDIR_DEFAULT)
