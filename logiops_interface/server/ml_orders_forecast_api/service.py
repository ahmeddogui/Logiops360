# server/ml_orders_forecast_api/service.py
from __future__ import annotations

import os
import io
import json
import sys
import subprocess
import pandas as pd
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

from flask import Blueprint, jsonify, make_response, request
from sqlalchemy import create_engine, text
from flask import current_app
from sqlalchemy.engine import URL  # pour construire l’URL proprement en fallback
import unicodedata

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in s.lower() if ch.isalnum())

def _find_col(cols, *candidates):
    """Essaie d'abord l’égalité (après normalisation), puis 'contient'."""
    slugs = { _slug(c): c for c in cols }
    # égalité
    for group in candidates:
        for cand in group:
            k = _slug(cand)
            if k in slugs:
                return slugs[k]
    # contient
    for group in candidates:
        for cand in group:
            k = _slug(cand)
            for col_slug, col_name in slugs.items():
                if k and (k in col_slug or col_slug in k):
                    return col_name
    return None



# ─────────────────────────── Paths & constantes ───────────────────────────
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))            # .../server/ml_orders_forecast_api
BASE_DIR   = os.path.dirname(MODULE_DIR)                            # .../server
ARTIFACT_DIR = os.path.join(BASE_DIR, "models", "orders_forecast")
os.makedirs(ARTIFACT_DIR, exist_ok=True)

PRED_DAILY_PATH = os.path.join(ARTIFACT_DIR, "predictions_daily.csv")
PRED_WEEK_PATH  = os.path.join(ARTIFACT_DIR, "predictions_weekly.csv")
PRED_MONTH_PATH = os.path.join(ARTIFACT_DIR, "predictions_monthly.csv")
METADATA_PATH   = os.path.join(ARTIFACT_DIR, "metadata.json")
OP_LOAD_CSV     = os.path.join(ARTIFACT_DIR, "operator_load_test_daily.csv")
PREVISION_SCRIPT= os.path.join(BASE_DIR, "models", "prevision_model.py")

bp = Blueprint("ml_orders_forecast", __name__, url_prefix="/api")

# ─────────────────────────── DB utils ───────────────────────────
def _db_url_from_env() -> str:
    if os.getenv("DATABASE_URL"):
        return os.getenv("DATABASE_URL")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "logiops")
    user = os.getenv("DB_USER", "postgres")
    pwd  = os.getenv("DB_PASS", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"

def get_engine():
    """Utilise l'engine de Flask si présent ; sinon fallback sûr depuis les env."""
    try:
        eng = current_app.config.get("_ENGINE")
        if eng is not None:
            return eng
    except Exception:
        pass  # pas dans un contexte Flask

    # Fallback: reconstruit une URL DB sans concaténer une chaîne (évite problèmes d'encodage)
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASS = os.getenv("DB_PASS", "mel")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "logiops")

    url = URL.create(
        "postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASS,   # URL.create gère les caractères spéciaux/accents
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    return create_engine(url, future=True, pool_pre_ping=True)


# ─────────────────────────── Helpers ───────────────────────────
def _json_no_cache(payload, status=200):
    resp = make_response(jsonify(payload), status)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    meta = (payload or {}).get("metadata") or {}
    if "snapshot_at" in meta:
        resp.headers["ETag"] = f"{meta.get('model_version','')}-{meta.get('snapshot_at','')}"
    return resp

def _read_meta():
    if os.path.exists(METADATA_PATH):
        return json.load(open(METADATA_PATH, "r", encoding="utf-8"))
    return {}

def _read_csv_safe(path: str) -> pd.DataFrame:
    """
    Standardise un fichier de prévision en 2 colonnes :
      ts   = datetime (ex: day)
      yhat = valeur prédite (ex: qty_pred)
    Colonnes supportées :
      - date: ts|date|ds|day|jour|creationdate|period|periodstart
      - pred: yhat|qty_pred|qtypred|pred|forecast|predicted|value|orders|commandes|qty|y
    """
    if not os.path.exists(path):
        return pd.DataFrame(columns=["ts", "yhat"])

    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame(columns=["ts", "yhat"])

    # colonne date
    ts_col = _find_col(
        df.columns,
        ("ts","date","ds","day","jour","creationdate","period","periodstart"),
    )
    if ts_col is None:
        return pd.DataFrame(columns=["ts", "yhat"])

    # colonne prédite
    y_col = _find_col(
        df.columns,
        ("yhat","qty_pred","qtypred","pred","forecast","predicted","value","orders","commandes","qty","y"),
    )

    df["ts"] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df[df["ts"].notna()].copy()

    if y_col:
        df["yhat"] = pd.to_numeric(df[y_col], errors="coerce")
    elif "yhat" in df.columns:
        df["yhat"] = pd.to_numeric(df["yhat"], errors="coerce")
    else:
        return pd.DataFrame(columns=["ts", "yhat"])

    return df[["ts","yhat"]].dropna().sort_values("ts")


# ─────────────────────────── Prévisions (aujourd’hui / demain / semaine / mois) ───────────────────────────
@bp.get("/forecast")
def get_forecast():
    meta = _read_meta()
    ddf  = _read_csv_safe(PRED_DAILY_PATH)
    wdf  = _read_csv_safe(PRED_WEEK_PATH)
    mdf  = _read_csv_safe(PRED_MONTH_PATH)

    now = pd.Timestamp.now(tz="Europe/Paris")
    today = now.normalize()
    tomorrow = today + pd.Timedelta(days=1)

    def _sum_for_date(df, d):
        if df.empty: return 0
        s = df.loc[df["ts"].dt.date.eq(d.date()), "yhat"].sum()
        if pd.notna(s) and s != 0: return int(round(s))
        last = df["ts"].max().date()
        return int(round(df.loc[df["ts"].dt.date.eq(last), "yhat"].sum()))

    # aujourd’hui / demain depuis le daily
    today_sum = _sum_for_date(ddf, today)
    tomorrow_sum = _sum_for_date(ddf, tomorrow) if not ddf.empty else 0

    # semaine : si pas de fichier weekly → on agrège le daily par ISO semaine
    if wdf.empty and not ddf.empty:
        iso = ddf["ts"].dt.isocalendar()
        wdf = ddf.assign(week=iso.week, year=ddf["ts"].dt.year) \
                 .groupby(["year","week"], as_index=False)["yhat"].sum() \
                 .assign(ts=pd.to_datetime(iso.year.astype(str) + "-W" + iso.week.astype(str) + "-1", errors="coerce"))

    week_sum = 0
    if not wdf.empty:
        iso = today.isocalendar()
        sel = wdf["ts"].dt.isocalendar()
        s = wdf.loc[(sel.week == iso.week) & (wdf["ts"].dt.year == iso.year), "yhat"].sum()
        if pd.notna(s) and s != 0:
            week_sum = int(round(s))
        else:
            last_ts = wdf["ts"].max()
            sel2 = wdf["ts"].dt.isocalendar()
            s2 = wdf.loc[(sel2.week == last_ts.isocalendar().week) & (wdf["ts"].dt.year == last_ts.year), "yhat"].sum()
            week_sum = int(round(s2)) if pd.notna(s2) else 0

    # mois : si pas de fichier monthly → on agrège le daily par mois
    if mdf.empty and not ddf.empty:
        mdf = ddf.assign(y=ddf["ts"].dt.year, m=ddf["ts"].dt.month) \
                 .groupby(["y","m"], as_index=False)["yhat"].sum() \
                 .assign(ts=pd.to_datetime(dict(year=lambda x: x["y"], month=lambda x: x["m"], day=1)))

    month_sum = 0
    if not mdf.empty:
        s = mdf.loc[(mdf["ts"].dt.month == today.month) & (mdf["ts"].dt.year == today.year), "yhat"].sum()
        if pd.notna(s) and s != 0:
            month_sum = int(round(s))
        else:
            last_ts = mdf["ts"].max()
            s2 = mdf.loc[(mdf["ts"].dt.month == last_ts.month) & (mdf["ts"].dt.year == last_ts.year), "yhat"].sum()
            month_sum = int(round(s2)) if pd.notna(s2) else 0

    return _json_no_cache({
        "today":      {"orders": today_sum,    "confidence": meta.get("confidence", 0.80)},
        "tomorrow":   {"orders": tomorrow_sum, "confidence": meta.get("confidence_tomorrow", 0.75)},
        "this_week":  {"orders": week_sum,     "confidence": meta.get("confidence_week", 0.80)},
        "this_month": {"orders": month_sum,    "confidence": meta.get("confidence_month", 0.78)},
        "metadata":   {
            "snapshot_at": now.isoformat(),
            "model_version": meta.get("model_version"),
            "trained_at": meta.get("trained_at"),
        },
    })


# ─────────────────────────── KPI (jour/semaine/charge) ───────────────────────────
@bp.get("/kpi/orders_summary")
def kpi_orders_summary():
    """day_orders / week_orders depuis DB si dispo ; avg_operator_load depuis CSV opérateurs."""
    import sys
    day_orders = 0
    week_orders = 0
    avg_operator_load = 0.0

    # DB (tolérant)
    try:
        eng = get_engine()
        with eng.connect() as cn:
            exists = cn.execute(text("SELECT to_regclass('public.clean_customer_orders')")).scalar()
            if exists:
                day_orders = cn.execute(text("""
                    SELECT COUNT(*)::int
                    FROM clean_customer_orders
                    WHERE DATE(creationdate AT TIME ZONE 'Europe/Paris') = CURRENT_DATE
                """)).scalar() or 0

                week_orders = cn.execute(text("""
                    SELECT COUNT(*)::int
                    FROM clean_customer_orders
                    WHERE DATE(creationdate AT TIME ZONE 'Europe/Paris') >= DATE_TRUNC('week', CURRENT_DATE)
                """)).scalar() or 0
    except Exception as e:
        print(f"[kpi_orders_summary] DB connect/query error: {e}", file=sys.stderr)

    # charge opérateurs depuis CSV (qty_real / qty_pred)
    try:
        if os.path.exists(OP_LOAD_CSV):
            df = pd.read_csv(OP_LOAD_CSV)
            pred_col = _find_col(df.columns, ("qty_pred","qtypred","pred","forecast","yhat","predictedqty"))
            act_col  = _find_col(df.columns, ("qty_real","qty","quantity","orders","commandes","actual","real","reel","y","actualqty"))
            if pred_col and act_col:
                num = pd.to_numeric(df[act_col], errors="coerce")
                den = pd.to_numeric(df[pred_col], errors="coerce")
                ratio = (num / den).replace([pd.NA, pd.NaT, float("inf")], 0).fillna(0)
                avg_operator_load = round(float(ratio.mean() * 100), 2)
    except Exception as e:
        print(f"[kpi_orders_summary] CSV error: {e}", file=sys.stderr)

    return _json_no_cache({
        "day_orders": int(day_orders),
        "week_orders": int(week_orders),
        "avg_operator_load": float(avg_operator_load),
    })

# ─────────────────────────── Charge opérateurs (depuis CSV) ───────────────────────────
@bp.get("/operators/load_status")
def operators_load_status():
    """
    Source opérateurs : models/orders_forecast/operator_load_test_daily.csv
      Colonnes attendues (mapping robuste): day/date, operator_id/name, qty_real, qty_pred
    Logique :
      1) Agréger PAR SEMAINE (ISO) les quantités réelles et prédites par opérateur
      2) Récupérer le total hebdo de commandes depuis la DB clean_customer_orders via engine
      3) Moyenne hebdo par opérateur = total_db_week / nb_operateurs
      4) Statut opérateur :
         - 'surcharge'    si orders > moyenne_par_operateur
         - 'sous-charge'  si (moyenne_par_operateur - orders) > 100
         - sinon 'active'
    Réponse :
      { items: [{ name, zone, orders, pct, status }], mean_orders_per_operator }
    """
    if not os.path.exists(OP_LOAD_CSV):
        return _json_no_cache({"items": [], "mean_orders_per_operator": 0})

    df = pd.read_csv(OP_LOAD_CSV)
    if df.empty:
        return _json_no_cache({"items": [], "mean_orders_per_operator": 0})

    # Mapping robuste
    date_col = _find_col(df.columns, ("day","date","jour","ts","ds"))
    name_col = _find_col(df.columns, ("operator_id","operator","operateur","name","agent","employee","opid","op_id"))
    pred_col = _find_col(df.columns, ("qty_pred","qtypred","pred","forecast","yhat","y_hat","predictedqty","forecastqty","expected"))
    act_col  = _find_col(df.columns, ("qty_real","qty","quantity","orders","commandes","actual","real","reel","y","actualqty"))

    if not (date_col and name_col and pred_col and act_col):
        return _json_no_cache({
            "items": [],
            "mean_orders_per_operator": 0,
            "debug": {"columns": list(df.columns)}
        })

    # Normalisation : DATE pure (pas de tz_localize ici)
    df["_d"]    = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df["_pred"] = pd.to_numeric(df[pred_col], errors="coerce").fillna(0)
    df["_act"]  = pd.to_numeric(df[act_col],  errors="coerce").fillna(0)
    df = df[df["_d"].notna()]
    if df.empty:
        return _json_no_cache({"items": [], "mean_orders_per_operator": 0})

    # ---- SEMAINE ISO courante (Europe/Paris) ----
    now_paris = pd.Timestamp.now(tz="Europe/Paris")
    iso_now   = now_paris.isocalendar()  # year, week, day
    iso_cal   = pd.to_datetime(df["_d"]).dt.isocalendar()  # colonnes: year, week, day
    df = df[(iso_cal["week"] == iso_now.week) & (iso_cal["year"] == iso_now.year)].copy()

    # fallback : 7 derniers jours si pas de données semaine courante
    if df.empty:
        seven_days_ago = (now_paris - pd.Timedelta(days=6)).date()
        df = df[df["_d"] >= seven_days_ago].copy()
        if df.empty:
            return _json_no_cache({"items": [], "mean_orders_per_operator": 0})

    # ---- Agrégation hebdo par opérateur ----
    grp = df.groupby(name_col, as_index=False).agg(
        pred=("_pred", "sum"),
        act=("_act",  "sum"),
    )

    # Zones fictives stables
    zones = ["Zone A", "Zone B", "Zone C", "Zone D", "Zone E", "Zone F"]
    grp["zone"] = [zones[i % len(zones)] for i in range(len(grp))]

    # Métriques
    grp["pred_safe"] = grp["pred"].replace(0, 1e-9)  # anti-division par zéro
    grp["pct"]    = ((grp["act"] / grp["pred_safe"]) * 100).clip(0, 100).round(0).astype(int)
    grp["orders"] = grp["act"].round(0).astype(int)

    # ---- Moyenne hebdo par opérateur depuis la DB ----
    mean_orders_per_operator = 0.0
    try:
        eng = get_engine()
        with eng.begin() as cn:
            total_week = cn.execute(text("""
                SELECT COUNT(*)::bigint
                FROM clean_customer_orders
                WHERE creationdate >= DATE_TRUNC('week', CURRENT_DATE)
                  AND creationdate <  DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '7 days'
            """)).scalar() or 0

        n_ops = int(len(grp))
        mean_orders_per_operator = float(total_week) / float(n_ops) if n_ops > 0 else 0.0

    except Exception as e:
        # fallback : moyenne empirique à partir des données opérateurs
        mean_orders_per_operator = float(grp["orders"].mean()) if len(grp) else 0.0
        print(f"[operators_load_status] DB mean fallback: {e}", file=sys.stderr)

    # ---- Statut par opérateur ----
    def _status(orders: float, mean_val: float) -> str:
        if orders > mean_val:
            return "surcharge"
        elif (mean_val - orders) > 100:
            return "sous-charge"
        return "active"

    grp["status"] = grp["orders"].apply(lambda x: _status(x, mean_orders_per_operator))

    items = [
        {
            "name": r[name_col],
            "zone": r["zone"],
            "orders": int(r["orders"]),
            "pct": int(r["pct"]),
            "status": r["status"],
        }
        for _, r in grp.iterrows()
    ]

    return _json_no_cache({
        "items": items,
        "mean_orders_per_operator": round(mean_orders_per_operator, 2)
    })



# ─────────────────────────── Upload + réentraîner via models/prevision_model.py ───────────────────────────
@bp.post("/orders/upload")
def upload_and_retrain():
    """
    1) Reçoit un CSV (form-data).
    2) Ajoute les données dans la table clean_customer_orders.
    3) Lance le script d'entraînement prevision_model.py.
    4) Retourne un message de succès.
    """
    import io, sys, subprocess

    # --- 1) Vérifier et lire le CSV ---
    if "file" not in request.files:
        current_app.logger.error(f"[upload_and_retrain] Erreur entraînement CSV manquant: {e}")
        return _json_no_cache({
        "error": f"Lancement entraînement échoué : {e}",
        "BASE_DIR": str(BASE_DIR)
    }, 500)


    f = request.files["file"]
    raw = f.read()
    if not raw:
        return _json_no_cache({"error": "CSV vide"}, 400)

    try:
        df = pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        current_app.logger.error(f"[upload_and_retrain] Erreur entraînement CSV manquant: {e}")
        return _json_no_cache({
        "error": f"Lancement entraînement échoué : {e}",
        "BASE_DIR": str(BASE_DIR)
    }, 400)

    if df.empty:
        return _json_no_cache({"error": "CSV sans données"}, 400)

    # --- 2) Insérer dans la base ---
    try:
        eng = get_engine()
        with eng.begin() as cn:
            cn.execute(text("""
                CREATE TABLE IF NOT EXISTS clean_customer_orders (
                    creationdate TIMESTAMP WITHOUT TIME ZONE
                )
            """))
            df.to_sql("clean_customer_orders", cn, if_exists="append", index=False)
    except Exception as e:
        current_app.logger.error(f"[upload_and_retrain] Erreur entraînement: {e}")
        return _json_no_cache({
        "error": f"Lancement entraînement échoué : {e}",
        "BASE_DIR": str(BASE_DIR)
    }, 500)

    # --- 3) Lancer le script d'entraînement ---
    try:
        BASE_DIR = Path(__file__).resolve().parent.parent  # ou parent.parent si tu veux
        script_path = BASE_DIR / "models" / "prevision_model.py"

        cmd = [sys.executable, str(script_path)]
        p = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=600)

        if p.returncode != 0:
            current_app.logger.error(f"[upload_and_retrain] Erreur entraînement CSV manquant: {e}")
            return _json_no_cache({
            "error": f"Lancement entraînement échoué : {e}",
            "BASE_DIR": str(BASE_DIR)
        }, 500)

    except Exception as e:
        current_app.logger.error(f"[upload_and_retrain] Erreur entraînement CSV manquant: {e}")
        return _json_no_cache({
        "error": f"Lancement entraînement échoué : {e}",
        "BASE_DIR": str(BASE_DIR)
    }, 500)

    # --- 4) Succès ---
    current_app.logger.info("[upload_and_retrain] Succès, retour JSON")
    return _json_no_cache({
    "status": "ok",
    "message": "Modèle réentraîné avec succès"
}, 200)

@bp.get("/_debug/forecast_files")
def _debug_forecast_files():
    out = {}
    for p in [PRED_DAILY_PATH, PRED_WEEK_PATH, PRED_MONTH_PATH]:
        k = os.path.basename(p)
        if os.path.exists(p):
            df = pd.read_csv(p, nrows=5)
            out[k] = {"exists": True, "columns": list(df.columns), "head": df.head(3).to_dict(orient="records")}
        else:
            out[k] = {"exists": False}
    return _json_no_cache(out)

@bp.get("/_debug/operators_headers")
def _debug_ops_headers():
    if not os.path.exists(OP_LOAD_CSV):
        return _json_no_cache({"exists": False})
    df = pd.read_csv(OP_LOAD_CSV, nrows=5)
    return _json_no_cache({"exists": True, "columns": list(df.columns), "head": df.head(3).to_dict(orient="records")})

