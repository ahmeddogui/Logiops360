import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import traceback
# -*- coding: utf-8 -*-

# Charger les variables du fichier .env
load_dotenv()

def create_database_if_not_exists():
    dbname = os.getenv("PG_DATABASE")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")

    if not all([dbname, user, password, host, port]):
        raise ValueError("Variables d'environnement manquantes.")

    try:
        conn = psycopg2.connect(dbname="postgres", user=user, password=password, host=host, port=port)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{format(dbname)}';")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {format(dbname)};")
            print(f"[✓] Base de données '{format(dbname)}' créée.")
        else:
            print(f"[i] Base de données '{format(dbname)}' existe déjà.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[✗] Erreur de création de la base : {e}")
        traceback.print_exc()

if __name__ == "__main__":
    create_database_if_not_exists()
