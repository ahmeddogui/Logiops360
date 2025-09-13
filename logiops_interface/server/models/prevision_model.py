import os
import json
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from joblib import dump

from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from xgboost import XGBRegressor
from prophet import Prophet

from utils.db_utils import connect_db

# === Fonctions métriques ===
def wape(y_true, y_pred):
    denom = np.maximum(np.abs(y_true).sum(), 1e-8)
    return float(np.abs(y_true - y_pred).sum() / denom)

def mae(y_true, y_pred):
    return float(np.mean(np.abs(y_true - y_pred)))

def rmse(y_true, y_pred):
    return float(np.sqrt(np.mean((y_true - y_pred)**2)))

# === Charger les données depuis la base ===
def load_data():
    eng = connect_db()
    df = pd.read_sql("SELECT * FROM clean_customer_orders", eng, parse_dates=["creationdate"])
    df.rename(columns={"creationdate": "day"}, inplace=True)
    return df

# === Feature engineering ===
def make_features(df):
    df = df.copy()
    df["day"] = pd.to_datetime(df["day"]).dt.normalize()

    # agrégation journalière
    agg = df.groupby(["day"], as_index=False).agg(
        qty_day=("quantity_units", "sum"),
        avg_size=("size_us", "mean"),
        n_orders=("ordernumber", "nunique")
    )

    # variables calendaires
    agg["dow"] = agg["day"].dt.dayofweek
    agg["week"] = agg["day"].dt.isocalendar().week.astype(int)
    agg["month"] = agg["day"].dt.month
    agg["is_month_start"] = agg["day"].dt.is_month_start.astype(int)
    agg["is_month_end"] = agg["day"].dt.is_month_end.astype(int)

    # lags et rollings
    for lag in [1, 7, 14, 30]:
        agg[f"lag{lag}"] = agg["qty_day"].shift(lag)
    for win in [3, 7, 14, 30]:
        agg[f"roll{win}"] = agg["qty_day"].rolling(win).mean().shift(1)

    agg["naive7"] = agg["lag7"]

    agg = agg.dropna().reset_index(drop=True)
    return agg

# === Split temporel ===
def split_data(df, ratio=0.8):
    unique_days = df["day"].unique()
    split_idx = int(len(unique_days) * ratio)
    split_day = unique_days[split_idx]
    train = df[df["day"] < split_day]
    test = df[df["day"] >= split_day]
    return train, test

# === Entraînement et comparaison ===
def train_models(train, test, outdir="model_outputs"):
    os.makedirs(outdir, exist_ok=True)

    X_train = train.drop(columns=["qty_day", "day"])
    y_train = train["qty_day"].values
    X_test = test.drop(columns=["qty_day", "day"])
    y_test = test["qty_day"].values

    models = {}
    results = []

    # Baselines
    results.append(["Baseline lag1", mae(y_test, test["lag1"]), wape(y_test, test["lag1"]), rmse(y_test, test["lag1"])])
    results.append(["Baseline lag7", mae(y_test, test["lag7"]), wape(y_test, test["lag7"]), rmse(y_test, test["lag7"])])

    # Random Forest
    rf = RandomForestRegressor(n_estimators=400, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    results.append(["RandomForest", mae(y_test, y_pred_rf), wape(y_test, y_pred_rf), rmse(y_test, y_pred_rf)])
    models["RandomForest"] = (rf, y_pred_rf)

    # Gradient Boosting
    try:
        gb = XGBRegressor(
            n_estimators=500, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            max_depth=6, random_state=42, n_jobs=-1,
            tree_method="hist"
        )
    except Exception:
        warnings.warn("XGBoost non dispo → HistGradientBoosting")
        gb = HistGradientBoostingRegressor(max_iter=600, learning_rate=0.06, random_state=42)

    gb.fit(X_train, y_train)
    y_pred_gb = gb.predict(X_test)
    results.append(["GradientBoosting", mae(y_test, y_pred_gb), wape(y_test, y_pred_gb), rmse(y_test, y_pred_gb)])
    models["GradientBoosting"] = (gb, y_pred_gb)

    # Prophet
    try:
        df_prophet = train[["day", "qty_day"]].rename(columns={"day": "ds", "qty_day": "y"})
        model_prophet = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False)
        model_prophet.fit(df_prophet)
        future = pd.DataFrame({"ds": test["day"]})
        forecast = model_prophet.predict(future)
        y_pred_prophet = forecast["yhat"].values
        results.append(["Prophet", mae(y_test, y_pred_prophet), wape(y_test, y_pred_prophet), rmse(y_test, y_pred_prophet)])
        models["Prophet"] = (model_prophet, y_pred_prophet)
    except Exception as e:
        print(f"⚠️ Prophet non utilisé : {e}")

    # Résultats comparatifs
    df_scores = pd.DataFrame(results, columns=["Model", "MAE", "WAPE", "RMSE"])
    df_scores.to_csv(os.path.join(outdir, "model_scores.csv"), index=False)

    # Choix du meilleur modèle
    best_row = df_scores.sort_values("MAE").iloc[0]
    best_model_name = best_row["Model"]
    best_model, y_pred_best = models.get(best_model_name, (None, test["lag1"].values))

    # Sauvegarde du meilleur modèle
    if best_model_name != "Prophet" and best_model is not None:
        dump(best_model, os.path.join(outdir, f"{best_model_name}.joblib"))

    # Graph comparaison modèles
    plt.figure(figsize=(8,4))
    plt.plot(test["day"], y_test, label="Réel", linewidth=2, color="black")
    for m, (_, yhat) in models.items():
        plt.plot(test["day"], yhat, label=m)
    plt.legend(); plt.title("Comparaison modèles vs réel")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "model_comparison.png"))
    plt.close()

    # Importance features
    if hasattr(best_model, "feature_importances_"):
        fi = pd.DataFrame({"Feature": X_train.columns, "Importance": best_model.feature_importances_})
        fi.sort_values("Importance", ascending=False).to_csv(os.path.join(outdir, "feature_importance.csv"), index=False)
        fi.head(15).plot(kind="bar", x="Feature", y="Importance", legend=False, figsize=(8,4), title="Feature importance")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "feature_importance.png"))
        plt.close()

    # KPI corrigés
    df_daily = pd.DataFrame({"day": test["day"], "qty_real": y_test, "qty_pred": y_pred_best})
    df_daily.to_csv(os.path.join(outdir, "predictions_daily.csv"), index=False)

    df_weekly = df_daily.resample("W-MON", on="day").sum().reset_index()
    df_weekly["ape"] = (df_weekly["qty_real"] - df_weekly["qty_pred"]).abs() / df_weekly["qty_real"].clip(lower=1e-8)
    df_weekly.to_csv(os.path.join(outdir, "predictions_weekly.csv"), index=False)

    df_monthly = df_daily.resample("M", on="day").sum().reset_index()
    df_monthly["ape"] = (df_monthly["qty_real"] - df_monthly["qty_pred"]).abs() / df_monthly["qty_real"].clip(lower=1e-8)
    df_monthly.to_csv(os.path.join(outdir, "predictions_monthly.csv"), index=False)

    # KPI globaux
    accuracy = 1 - df_daily["qty_real"].sub(df_daily["qty_pred"]).abs().div(df_daily["qty_real"].clip(lower=1e-8)).mean()
    service_level = (df_daily["qty_real"].sub(df_daily["qty_pred"]).abs().div(df_daily["qty_real"].clip(lower=1e-8)) <= 0.20).mean()

    metadata = {
        "best_model": best_model_name,
        "metrics": df_scores.to_dict(orient="records"),
        "kpi": {"accuracy": accuracy, "service_level_20": service_level},
        "train_start": str(train["day"].min()),
        "train_end": str(train["day"].max()),
        "test_start": str(test["day"].min()),
        "test_end": str(test["day"].max()),
        "n_train": len(train),
        "n_test": len(test)
    }
    with open(os.path.join(outdir, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    print("Entraînement terminé. Résultats dans", outdir)

# === Main ===
if __name__ == "__main__":
    df = load_data()
    df_feat = make_features(df)
    train, test = split_data(df_feat)
    train_models(train, test, outdir="models/orders_forecast")
