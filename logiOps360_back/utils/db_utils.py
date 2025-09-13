import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    dbname = os.getenv("PG_DATABASE")

    if not all([user, password, host, port, dbname]):
        raise ValueError("Une ou plusieurs variables d'environnement sont manquantes.")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    return engine
