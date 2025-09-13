from __future__ import annotations

import os
from datetime import timedelta

from ml_eta_api import bp_eta
from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager,
    create_access_token,
    get_jwt_identity,
    jwt_required,
)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from passlib.hash import bcrypt
import pandas as pd

from models import Base, User, TypeProfil
from ml_reco_simple_api import bp_reco_simple
from ml_delay_api import bp_delay
from ml_anomaly_api import bp_anom
from kpi_api import bp_kpi
from ml_orders_forecast_api import bp_orders_forecast
from storage_api import bp as bp_storage


# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "kdh")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "logiops")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-me-in-prod")

# ----------------------------------------------------------------------------
# App / DB / Auth setup
# ----------------------------------------------------------------------------
app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = JWT_SECRET_KEY
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=12)

CORS(
    app,
    resources={r"/api/*": {"origins": ["http://localhost:8080", "http://127.0.0.1:8080"]}},
    supports_credentials=True,
    allow_headers=["*"],
    expose_headers=["*"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
)

jwt = JWTManager(app)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))

# ----------------------------------------------------------------------------
# Register blueprints
# ----------------------------------------------------------------------------
app.config["_ENGINE"] = engine
app.register_blueprint(bp_eta)
app.register_blueprint(bp_reco_simple)
app.register_blueprint(bp_delay)
app.register_blueprint(bp_anom)
app.register_blueprint(bp_kpi)
app.register_blueprint(bp_orders_forecast)
app.register_blueprint(bp_storage)

print("\n=== ROUTES DISPONIBLES ===")
for rule in app.url_map.iter_rules():
    methods = ",".join(sorted(rule.methods))
    print(f"{rule.endpoint:30s} {methods:20s} {rule.rule}")
print("====================================\n")

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
ALLOWED_PROFILES = {p.value for p in TypeProfil}


def validate_profile(value: str) -> str:
    v = (value or "").strip().lower()
    if v not in ALLOWED_PROFILES:
        raise ValueError(
            f"type_profil invalide. Attendu: {', '.join(sorted(ALLOWED_PROFILES))}"
        )
    return v

# ----------------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------------

@app.get("/api/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("select 1"))
        return jsonify(status="ok"), 200
    except Exception as e:
        return jsonify(status="error", error=str(e)), 500


@app.get("/api/supervisor/charts")
def supervisor_charts():
    """Données pour le dashboard commandes"""
    try:
        with engine.connect() as conn:
            orders_trend_sql = """
                SELECT 
                    DATE(creationdate) as date,
                    COUNT(*) as orders_count,
                    COUNT(DISTINCT operator) as operators_count
                FROM clean_customer_orders 
                WHERE creationdate >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(creationdate)
                ORDER BY date
            """
            orders_trend = [dict(row._mapping) for row in conn.execute(text(orders_trend_sql)).fetchall()]

            customer_orders_sql = """
                SELECT 
                    codcustomer,
                    COUNT(*) as orders_count,
                    SUM(quantity_units) as total_quantity
                FROM clean_customer_orders
                WHERE DATE_TRUNC('year', creationdate) = DATE_TRUNC('year', CURRENT_DATE)
                GROUP BY codcustomer
                ORDER BY orders_count DESC
                LIMIT 10
            """
            customer_orders = [dict(row._mapping) for row in conn.execute(text(customer_orders_sql)).fetchall()]

            size_distribution_sql = """
                SELECT 
                    size_us,
                    COUNT(*) as orders_count,
                    SUM(quantity_units) as total_quantity
                FROM clean_customer_orders 
                WHERE DATE(creationdate) = CURRENT_DATE
                AND size_us IS NOT NULL
                GROUP BY size_us
                ORDER BY orders_count DESC
            """
            size_distribution = [dict(row._mapping) for row in conn.execute(text(size_distribution_sql)).fetchall()]

            operator_performance_sql = """
                SELECT 
                    operator,
                    COUNT(*) as orders_processed,
                    SUM(quantity_units) as total_units,
                    COUNT(DISTINCT wavenumber) as waves_handled
                FROM clean_customer_orders 
                WHERE DATE(creationdate) = CURRENT_DATE
                AND operator IS NOT NULL
                GROUP BY operator
                ORDER BY orders_processed DESC
                LIMIT 15
            """
            operator_performance = [dict(row._mapping)
                                    for row in conn.execute(text(operator_performance_sql)).fetchall()]

        return jsonify(
            orders_trend=orders_trend,
            customer_orders=customer_orders,
            size_distribution=size_distribution,
            operator_performance=operator_performance
        ), 200

    except Exception as e:
        return jsonify(error=str(e)), 500


@app.get("/api/storage/analytics")
def storage_analytics():
    """Données analytiques pour le dashboard Stockage"""
    try:
        with engine.connect() as conn:
            class_sql = """
                SELECT class, COUNT(*) as nb_products, SUM(quantity) as total_qty
                FROM public.clean_class_based_storage
                GROUP BY class
                ORDER BY nb_products DESC
            """
            class_distribution = [dict(row._mapping) for row in conn.execute(text(class_sql)).fetchall()]

            top_storage_sql = """
                SELECT sp.label,
                       SUM(sl.volume) as total_volume,
                       MAX(sp.x_coord) as x_coord,
                       MAX(sp.y_coord) as y_coord,
                       MAX(sp.z_coord) as z_coord,
                       MAX(sp.norm) as norm
                FROM public.clean_storage_location sl
                JOIN public.clean_support_points sp
                  ON sl.support_label = sp.label
                GROUP BY sp.label
                ORDER BY total_volume DESC
                LIMIT 10
            """
            top_storage_points = [dict(row._mapping) for row in conn.execute(text(top_storage_sql)).fetchall()]

        return jsonify(
            class_distribution=class_distribution,
            top_storage_points=top_storage_points
        ), 200
    except Exception as e:
        return jsonify(error=str(e)), 500


@app.get("/api/transport/charts")
def transport_charts():
    """Données analytiques pour le dashboard transport"""
    try:
        with engine.connect() as conn:
            # Livraisons par zone de destination
            deliveries_sql = """
                SELECT 
                    destination_zone,
                    COUNT(*) AS deliveries_count
                FROM public.shipments
                WHERE delivery_datetime IS NOT NULL
                GROUP BY destination_zone
                ORDER BY deliveries_count DESC
            """
            deliveries_by_zone = [dict(row._mapping) for row in conn.execute(text(deliveries_sql)).fetchall()]

            # Frais moyens par transporteur
            cost_sql = """
                SELECT 
                    carrier,
                    ROUND(AVG(cost_estimated),2) AS avg_cost
                FROM public.shipments
                WHERE cost_estimated IS NOT NULL
                GROUP BY carrier
                ORDER BY avg_cost DESC
            """
            avg_cost_by_carrier = [dict(row._mapping) for row in conn.execute(text(cost_sql)).fetchall()]

        return jsonify(
            deliveries_by_zone=deliveries_by_zone,
            avg_cost_by_carrier=avg_cost_by_carrier
        ), 200
    except Exception as e:
        return jsonify(error=str(e)), 500


# ----------------------------------------------------------------------------
# Auth routes avec gestion OPTIONS
# ----------------------------------------------------------------------------

@app.route("/api/auth/login", methods=["POST", "OPTIONS"])
def login():
    if request.method == "OPTIONS":
        # Réponse CORS pour le préflight
        resp = make_response("", 200)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    data = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or data.get("mot_de_passe") or ""
    type_profil = data.get("type_profil") or data.get("profile") or ""

    try:
        type_profil = validate_profile(type_profil)
    except ValueError as ve:
        return jsonify(message=str(ve)), 400

    session = SessionLocal()
    try:
        user = (
            session.query(User)
            .filter(User.email == email, User.type_profil == type_profil)
            .first()
        )
        if not user or not bcrypt.verify(password, user.mot_de_passe_hash):
            return jsonify(message="Identifiants invalides"), 401

        token = create_access_token(
            identity=str(user.id),
            additional_claims={"email": user.email, "type_profil": user.type_profil, "nom": user.nom},
        )
        return jsonify(id=str(user.id), nom=user.nom, email=user.email, type_profil=user.type_profil, token=token), 200
    except Exception as e:
        return jsonify(message="Erreur serveur", error=str(e)), 500
    finally:
        session.close()


@app.route("/api/auth/signup", methods=["POST", "OPTIONS"])
def signup():
    if request.method == "OPTIONS":
        resp = make_response("", 200)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    data = request.get_json(force=True) or {}
    nom = (data.get("nom") or "").strip()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or data.get("mot_de_passe") or ""
    type_profil = data.get("type_profil") or data.get("profile") or ""

    if not nom or not email or not password or not type_profil:
        return jsonify(message="Champs requis manquants"), 400

    try:
        type_profil = validate_profile(type_profil)
    except ValueError as ve:
        return jsonify(message=str(ve)), 400

    session = SessionLocal()
    try:
        existing = (
            session.query(User)
            .filter(User.email == email, User.type_profil == type_profil)
            .first()
        )
        if existing:
            return jsonify(message="Un utilisateur avec cet email et ce profil existe déjà"), 409

        pwd_hash = bcrypt.hash(password)
        user = User(nom=nom, email=email, mot_de_passe_hash=pwd_hash, type_profil=type_profil)
        session.add(user)
        session.commit()
        session.refresh(user)

        token = create_access_token(
            identity=str(user.id),
            additional_claims={"email": user.email, "type_profil": user.type_profil, "nom": user.nom},
        )
        return jsonify(id=str(user.id), nom=user.nom, email=user.email, type_profil=user.type_profil, token=token), 201
    except Exception as e:
        session.rollback()
        return jsonify(message="Erreur serveur", error=str(e)), 500
    finally:
        session.close()


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def init_db() -> None:
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init_db()
    app.run(host="127.0.0.1", port=8000, debug=True)
