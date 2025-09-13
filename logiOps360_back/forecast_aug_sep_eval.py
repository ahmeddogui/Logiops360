#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.db_utils import connect_db

# On tente d'utiliser ta classe existante (patchée) :
try:
    from Commandes.Models.forecasts import DemandForecaster
except Exception:
    # fallback si tu lances depuis le dossier Models
    from forecasts import DemandForecaster

import pandas as pd
import numpy as np
from datetime import timedelta

OUTDIR = Path("outputs")
TARGET_YEAR = 2025                  # <-- adapte si besoin
CUTOFF_DATE = pd.Timestamp(f"{TARGET_YEAR}-08-01")  # on entraîne avant cette date (exclue)
END_DATE = pd.Timestamp(f"{TARGET_YEAR}-10-01")     # on évalue jusqu'à < 1er oct. (août+septembre)

def mae(ae):
    return float(np.mean(ae)) if len(ae) else np.nan

def wape(abs_err, actual):
    den = np.sum(actual)
    return float(np.sum(abs_err) / den) if den else np.nan

def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    eng = connect_db()

    # 1) Charger l'agrégat hebdo (reference, week, qty)
    forecaster = DemandForecaster(
        engine=eng,
        horizon_weeks=8,            # sera recalculé
        min_history_weeks=8,
        top_refs=None,
        table_raw="clean_customer_orders",
        val_weeks=0,                # on gère nous-mêmes la fenêtre d'éval
        write_back=False            # on n'écrit pas en BDD pour cette expérience
    )
    agg = forecaster.load()  # [reference, week (Lundi), qty]

    # 2) Split: train < cutoff, eval ∈ [cutoff, end)
    train_hist = agg[agg["week"] < CUTOFF_DATE].copy()
    eval_actual = agg[(agg["week"] >= CUTOFF_DATE) & (agg["week"] < END_DATE)].copy()

    if train_hist.empty:
        print("❗Pas d'historique avant la date de coupure. Ajuste CUTOFF_DATE / données.")
        return

    # 3) Fit modèle sur train uniquement
    X, y, _ = forecaster._prep_supervised(train_hist)  # utilise ta préparation interne (lags + time)
    if X.empty:
        print("❗Historique insuffisant pour constituer X (lags manquants).")
        return
    forecaster.fit(X, y)

    # 4) Déterminer l'horizon nécessaire pour couvrir jusqu'à END_DATE
    last_train_week = train_hist["week"].max()  # Monday of last train week
    # nombre de semaines entre (last_train_week + 7j) et END_DATE (exclu)
    horizon_weeks = int(np.ceil((END_DATE - (last_train_week + pd.Timedelta(days=7))).days / 7.0))
    horizon_weeks = max(horizon_weeks, 0)

    # 5) Rolling forecast à partir du train, jusqu'à END_DATE
    pred_all = forecaster.rolling_forecast(train_hist, horizon=horizon_weeks)
    # garder uniquement les semaines d'intérêt (août & septembre)
    pred_eval = pred_all[(pred_all["week"] >= CUTOFF_DATE) & (pred_all["week"] < END_DATE)].copy()

    # 6) Jointure hebdo pour AOÛT+SEPT
    join_w = (eval_actual.rename(columns={"qty":"qty_actual"})
              .merge(pred_eval.rename(columns={"qty":"qty_pred"}),
                     on=["reference","week"], how="inner"))
    join_w["ae"] = (join_w["qty_pred"] - join_w["qty_actual"]).abs()

    # 7) Métriques hebdo (sur l’ensemble août+septembre)
    mae_weekly = mae(join_w["ae"])
    wape_weekly = wape(join_w["ae"], join_w["qty_actual"])

    # 8) Métriques mensuelles (agrégé par mois)
    # passage en mois
    join_w["month"] = join_w["week"].dt.to_period("M").dt.to_timestamp()
    monthly_pred = join_w.groupby("month", as_index=False)["qty_pred"].sum()
    monthly_act  = join_w.groupby("month", as_index=False)["qty_actual"].sum()
    join_m = monthly_act.merge(monthly_pred, on="month", how="inner")
    join_m["ae_month"] = (join_m["qty_pred"] - join_m["qty_actual"]).abs()
    mae_monthly = mae(join_m["ae_month"])
    wape_monthly = wape(join_m["ae_month"], join_m["qty_actual"])

    # 9) Sauvegardes
    join_w.to_csv(OUTDIR / "aug_sep_forecast_weekly.csv", index=False)
    join_m.to_csv(OUTDIR / "aug_sep_forecast_monthly.csv", index=False)

    synth = pd.DataFrame([{
        "cutoff_date": CUTOFF_DATE.date(),
        "eval_start": CUTOFF_DATE.date(),
        "eval_end_excl": END_DATE.date(),
        "weeks_eval_n": int(join_w["week"].nunique()),
        "pairs_eval_n": int(len(join_w)),
        "MAE_weekly": mae_weekly,
        "WAPE_weekly": wape_weekly,
        "MAE_monthly": mae_monthly,
        "WAPE_monthly": wape_monthly
    }])
    synth.to_csv(OUTDIR / "aug_sep_metrics.csv", index=False)

    # 10) Affichage console
    print("\n=== Évaluation Août & Septembre ===")
    print(synth.to_string(index=False))
    print("\nDétails enregistrés dans:", OUTDIR)

if __name__ == "__main__":
    main()
