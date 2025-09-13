from __future__ import annotations
import os
from typing import Dict, List
import numpy as np
import pandas as pd
from flask import current_app
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ----------------- Engine -----------------
def get_engine():
    """Récupère l'engine Flask ou reconstruit depuis les variables d'env."""
    try:
        eng = current_app.config.get("_ENGINE")
        if eng is not None:
            return eng
    except Exception:
        pass
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASS = os.getenv("DB_PASS", "mel")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "logiops")
    url = URL.create(
        "postgresql+psycopg2",
        username=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
    )
    return create_engine(url, future=True, pool_pre_ping=True)

# ----------------- Loaders DB -----------------
def load_unified() -> pd.DataFrame:
    """Lit unified_storage_view et calcule on_hand côté SQL."""
    eng = get_engine()
    q = text("""
        SELECT
          location,
          referenceproduit,
          COALESCE(qty_class_based,0)   AS qty_class_based,
          COALESCE(qty_dedicated,0)     AS qty_dedicated,
          COALESCE(qty_random,0)        AS qty_random,
          COALESCE(qty_class_based,0)+COALESCE(qty_dedicated,0)+COALESCE(qty_random,0) AS on_hand
        FROM unified_storage_view
    """)
    return pd.read_sql_query(q, eng)

def _normalize_support_label(df: pd.DataFrame) -> pd.DataFrame:
    """Crée/renomme la colonne 'support_label' selon ce qu'on trouve en base."""
    lower = {c: c.lower() for c in df.columns}
    df = df.rename(columns=lower)
    candidates: List[str] = [
        "support_label", "label", "support", "supportcode", "support_code",
        "name", "supportlabel", "support_id", "supportid"
    ]
    for c in candidates:
        if c in df.columns:
            if c != "support_label":
                df = df.rename(columns={c: "support_label"})
            break
    else:
        df["support_label"] = np.nan
    return df

def _normalize_xy_latlon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assure la présence de colonnes x,y,z si disponibles (x_coord/y_coord/z_coord/x/y).
    Laisse lat/lon si existent, sinon None.
    """
    lower = {c: c.lower() for c in df.columns}
    df = df.rename(columns=lower)

    # XY
    if "x" not in df.columns:
        if "x_coord" in df.columns:
            df = df.rename(columns={"x_coord": "x"})
        elif "longitude" in df.columns or "lng" in df.columns:
            df = df.rename(columns={"longitude": "x"}) if "longitude" in df.columns else df.rename(columns={"lng": "x"})
        else:
            df["x"] = np.nan

    if "y" not in df.columns:
        if "y_coord" in df.columns:
            df = df.rename(columns={"y_coord": "y"})
        elif "latitude" in df.columns:
            df = df.rename(columns={"latitude": "y"})
        else:
            df["y"] = np.nan

    if "z" not in df.columns:
        if "z_coord" in df.columns:
            df = df.rename(columns={"z_coord": "z"})
        else:
            df["z"] = np.nan

    # lat/lon (optionnels, si jamais présents)
    if "lat" not in df.columns:
        df["lat"] = np.nan
    if "lon" not in df.columns:
        df["lon"] = np.nan

    return df

def load_locations() -> pd.DataFrame:
    """clean_storage_location normalisé (support_label assuré)."""
    eng = get_engine()
    df = pd.read_sql_query(text("SELECT * FROM clean_storage_location"), eng)
    df = _normalize_support_label(df)
    return df

def load_supports() -> pd.DataFrame:
    """
    clean_support_points normalisé.
    Cas réel fourni: colonnes = label, x_coord, y_coord, z_coord, norm
    -> on sort: support_label, x, y, z, norm, lat, lon
    """
    eng = get_engine()
    df = pd.read_sql_query(text("SELECT * FROM clean_support_points"), eng)
    df = _normalize_support_label(df)
    df = _normalize_xy_latlon(df)
    # garde norm si présent
    if "norm" not in df.columns:
        df["norm"] = np.nan
    return df

def join_unified_locations() -> pd.DataFrame:
    """unified_storage_view ⨝ clean_storage_location (pour support_label)."""
    uni = load_unified()
    loc = load_locations()
    out = uni.merge(loc[["location","support_label"]].drop_duplicates(),
                    on="location", how="left")
    return out

# ----------------- Capacités & agrégations -----------------
def capacity_map(df_uni_loc: pd.DataFrame, default_per_slot: float = 60.0) -> Dict[str, float]:
    """
    Capacité estimée par support_label = P95 des on_hand agrégés par location.
    Fallback sur default_per_slot si pas assez de données.
    """
    tmp = (df_uni_loc.groupby(["support_label","location"], dropna=False)["on_hand"]
           .sum().reset_index())
    cap = (tmp.groupby("support_label")["on_hand"]
             .quantile(0.95)
             .fillna(default_per_slot)
             .clip(lower=1.0)
             .to_dict())
    if not cap:
        cap = {"_default": default_per_slot}
    return cap

def make_location_agg(df_uni_loc: pd.DataFrame, cap_map: Dict[str, float]) -> pd.DataFrame:
    agg = df_uni_loc.groupby(["location","support_label"], dropna=False).agg(
        on_hand=("on_hand","sum"),
        n_skus=("referenceproduit","nunique"),
        dedicated=("qty_dedicated","sum"),
        random_qty=("qty_random","sum"),
        class_based=("qty_class_based","sum"),
    ).reset_index()
    med = np.median(list(cap_map.values()) or [60.0])
    agg["loc_capacity"] = agg["support_label"].map(cap_map).fillna(med)
    agg["occ_ratio"] = (agg["on_hand"] / agg["loc_capacity"]).replace([np.inf,-np.inf],0).fillna(0.0)
    return agg

def infer_zone_from_location(loc: str) -> str:
    return (loc or " ").strip()[0:1].upper() if isinstance(loc,str) else "?"

def make_zone_agg(loc_agg: pd.DataFrame) -> pd.DataFrame:
    zagg = loc_agg.copy()
    zagg["zone"] = zagg["location"].astype(str).map(infer_zone_from_location)
    z = zagg.groupby("zone").agg(
        n_locations=("location","nunique"),
        on_hand=("on_hand","sum"),
        capacity=("loc_capacity","sum"),
    ).reset_index()
    z["occupancy_pct"] = (z["on_hand"]/z["capacity"]).replace([np.inf,-np.inf],0).fillna(0.0)
    z["status"] = np.where(z["occupancy_pct"]>=0.90,"critique",
                   np.where(z["occupancy_pct"]>=0.80,"alerte","ok"))
    return z

def velocity_proxy(df_uni_loc: pd.DataFrame) -> pd.DataFrame:
    """
    Proxy de vélocité sans historique :
    vel = 0.6 * rang_pct(on_hand par SKU) + 0.4 * part_dedicated(SKU)
    """
    sku = df_uni_loc.groupby("referenceproduit").agg(
        on_hand=("on_hand","sum"),
        dedicated=("qty_dedicated","sum"),
        total=("on_hand","sum"),
    ).reset_index()
    sku["share_dedicated"] = (sku["dedicated"]/sku["total"]).replace([np.inf,-np.inf],0).fillna(0.0)
    sku["rank_onhand"] = sku["on_hand"].rank(pct=True, method="average")
    sku["vel_score"] = 0.6*sku["rank_onhand"] + 0.4*sku["share_dedicated"]
    return sku[["referenceproduit","vel_score","on_hand"]]
