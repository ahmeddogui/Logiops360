from flask import Flask, jsonify
import random
import datetime
import string
import os
import pandas as pd

try:
    from utils.db_utils import get_engine
except Exception:
    get_engine = None

app = Flask(__name__)

REFERENCE_POOL = []

def load_reference_pool():
    global REFERENCE_POOL
    refs = []
    if get_engine is not None:
        eng = get_engine()
        for table in ["clean_product", "product"]:
            try:
                df = pd.read_sql(f'SELECT "Reference" FROM {table}', eng)
                refs = df["Reference"].dropna().astype(str).str.strip().str.upper().unique().tolist()
                if refs:
                    break
            except Exception:
                continue
    if not refs:
        refs = [''.join(random.choices(string.ascii_uppercase + string.digits, k=6)) for _ in range(500)]
    REFERENCE_POOL = refs

load_reference_pool()

def generate_codCustomer():
    return f"C{str(random.randint(1, 9999999)).zfill(7)}"

def generate_orderNumber():
    return random.randint(100000, 999999)

def generate_orderToCollect():
    return random.randint(1, 10)

def generate_reference():
    return random.choice(REFERENCE_POOL)

def generate_size():
    return float(random.choice([7, 8, 9, 10, 11, 12, 13, 14, 15, 95, 105]))

def generate_quantity():
    return random.randint(1, 10)

def generate_creationDate():
    now = datetime.datetime.now()
    return now.strftime("%d/%m/%Y %H:%M")

def generate_waveNumber():
    return random.randint(40000, 50000)

def generate_operator():
    return f"Operator_{random.randint(1, 10)}"

def generate_fake_order():
    return {
        "codCustomer": generate_codCustomer(),
        "orderNumber": generate_orderNumber(),
        "orderToCollect": generate_orderToCollect(),
        "Reference": generate_reference(),
        "Size (US)": generate_size(),
        "quantity (units)": generate_quantity(),
        "creationDate": generate_creationDate(),
        "waveNumber": generate_waveNumber(),
        "operator": generate_operator()
    }

@app.route("/new_orders", methods=["GET"])
def new_orders():
    orders = [generate_fake_order() for _ in range(100)]
    return jsonify(orders)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
