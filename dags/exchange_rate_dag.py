import json
import sqlite3
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/raw_rates.json"
CLEAN_FILE = f"{DATA_DIR}/clean_rates.json"
DB_FILE = f"{DATA_DIR}/rates.db"


def extract():
    response = requests.get("https://open.er-api.com/v6/latest/USD", timeout=30)
    response.raise_for_status()
    with open(RAW_FILE, "w") as f:
        json.dump(response.json(), f)


def transform():
    with open(RAW_FILE) as f:
        raw = json.load(f)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    rows = []
    for currency in ["USD", "LKR", "EUR", "GBP"]:
        rows.append({"date": today, "currency": currency, "rate": raw["rates"][currency]})
    with open(CLEAN_FILE, "w") as f:
        json.dump(rows, f)


def load():
    with open(CLEAN_FILE) as f:
        rows = json.load(f)
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS exchange_rates ("
        "date TEXT, currency TEXT, rate REAL, PRIMARY KEY (date, currency))"
    )
    for row in rows:
        conn.execute(
            "INSERT OR IGNORE INTO exchange_rates VALUES (?, ?, ?)",
            (row["date"], row["currency"], row["rate"]),
        )
    conn.commit()
    conn.close()


with DAG(
    dag_id="exchange_rate_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task