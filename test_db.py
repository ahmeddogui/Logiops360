import os, psycopg2, sys
print("DB_HOST =", os.getenv("DB_HOST"))
print("DB_NAME =", os.getenv("DB_NAME"))
print("DB_USER =", os.getenv("DB_USER"))
print("DB_PORT =", os.getenv("DB_PORT"))

try:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=int(os.getenv("DB_PORT") or 5432),
        connect_timeout=5,
    )
    print("Connected to Postgres")
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    print("Simple query OK:", cur.fetchone())
    conn.close()
except Exception as e:
    print("DB connect error:", repr(e))
    sys.exit(1)
