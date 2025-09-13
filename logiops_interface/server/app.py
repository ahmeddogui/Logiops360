from __future__ import annotations

import os
from datetime import timedelta

from ml_eta_api import bp_eta
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager,
    create_access_token,
    get_jwt_identity,
    jwt_required,
)
from sqlalchemy import create_engine, text
# sqlalchemy.orm
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
CORS_ORIGIN = os.getenv("CORS_ORIGIN", "http://localhost:8080")

# ----------------------------------------------------------------------------
# App / DB / Auth setup
# ----------------------------------------------------------------------------
app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = JWT_SECRET_KEY
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=12)

# CORS (autorise localhost ET 127.0.0.1 + header Authorization)
CORS(
    app,
    resources={
        r"/api/*": {
            "origins": ["http://localhost:8080", "http://127.0.0.1:8080", CORS_ORIGIN]
        }
    },
    supports_credentials=True,
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization","Cache-Control"],
    expose_headers=["Content-Type", "Authorization"]
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
    """Récupère les données pour les graphiques du dashboard superviseur"""
    try:
        with engine.connect() as conn:
            # Graphe 1 : Top 10 points de stockage par volume
            top_storage_sql = """
                SELECT l.label, SUM(sl.volume) as total_volume
                FROM public.clean_support_points l
                JOIN public.clean_storage_location sl
                  ON l.label = sl.support_label
                GROUP BY l.label
                ORDER BY total_volume DESC
                LIMIT 10
            """
            top_storage = [dict(row._mapping) for row in conn.execute(text(top_storage_sql)).fetchall()]

            # Graphe 2 : Vue des stocks (quantité totale) sans valeurs à zéro
            stock_view_sql = """
                SELECT referenceproduit,
                       (qty_class_based + qty_dedicated + qty_random) as total_qty
                FROM public.unified_storage_view
                WHERE (qty_class_based + qty_dedicated + qty_random) > 0
                ORDER BY total_qty DESC
            """
            stock_view = [dict(row._mapping) for row in conn.execute(text(stock_view_sql)).fetchall()]

            # Données des commandes par jour (derniers 7 jours)
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

            # Répartition par client
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

            # Volume par taille
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

            # Performance par opérateur
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
                operator_performance=operator_performance,
                top_storage=top_storage,
                stock_view=stock_view
            ), 200

    except Exception as e:
        return jsonify(error=str(e)), 500

@app.post("/api/auth/signup")
def signup():
    data = request.get_json(force=True) or {}
    # 🔹 Log côté serveur pour vérifier ce que Flask reçoit
    print("Données reçues pour signup:", data)
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
            return (
                jsonify(message="Un utilisateur avec cet email et ce profil existe déjà"),
                409,
            )

        pwd_hash = bcrypt.hash(password)
        user = User(
            nom=nom,
            email=email,
            mot_de_passe_hash=pwd_hash,
            type_profil=type_profil,
        )
        session.add(user)
        session.commit()
        session.refresh(user)

        token = create_access_token(
            identity=str(user.id),
            additional_claims={
                "email": user.email,
                "type_profil": user.type_profil,
                "nom": user.nom,
            },
        )
        return (
            jsonify(
                id=str(user.id),
                nom=user.nom,
                email=user.email,
                type_profil=user.type_profil,
                token=token,
            ),
            201,
        )
    except Exception as e:
        session.rollback()
        return jsonify(message="Erreur serveur", error=str(e)), 500
    finally:
        session.close()


@app.post("/api/auth/login")
def login():
    data = request.get_json(force=True) or {}

    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or data.get("mot_de_passe") or ""
    type_profil = data.get("type_profil") or data.get("profile") or ""

    if not email or not password or not type_profil:
        return jsonify(message="Champs requis manquants"), 400

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
            additional_claims={
                "email": user.email,
                "type_profil": user.type_profil,
                "nom": user.nom,
            },
        )
        return (
            jsonify(
                id=str(user.id),
                nom=user.nom,
                email=user.email,
                type_profil=user.type_profil,
                token=token,
            ),
            200,
        )
    except Exception as e:
        return jsonify(message="Erreur serveur", error=str(e)), 500
    finally:
        session.close()


@app.get("/api/auth/me")
@jwt_required()
def me():
    user_id = get_jwt_identity()
    session = SessionLocal()
    try:
        user = session.get(User, user_id)
        if not user:
            return jsonify(message="Utilisateur introuvable"), 404
        return (
            jsonify(
                id=str(user.id),
                nom=user.nom,
                email=user.email,
                type_profil=user.type_profil,
                date_creation=user.date_creation.isoformat(),
            ),
            200,
        )
    finally:
        session.close()

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------


def init_db() -> None:
    """Créer les tables si elles n'existent pas encore."""
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init_db()
    app.run(host="127.0.0.1", port=8000, debug=True)
