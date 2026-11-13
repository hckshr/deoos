"""Daily paid-revenue DAG, Airflow 2.11.

Logical date comes from the cron schedule ``0 6 * * *``. catchup is off.
max_active_runs=1 is the overlap control. retries=1 is one retry after the
first failure. The deoos rewrite is daily_revenue.py. The comparison is in
README.md.
"""
import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


def read_orders(path):
    with open(path, encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    cleaned = []
    for row in rows:
        cleaned.append({
            "order_id": row["order_id"],
            "day": row["day"],
            "status": row["status"],
            "cents": int(row["cents"]),
        })
    return cleaned


def revenue(rows):
    paid = [row for row in rows if row["status"] == "paid"]
    return {"paid_orders": len(paid), "cents": sum(row["cents"] for row in paid)}


def extract_orders(ds, **_):
    return [row for row in read_orders(os.environ["ORDERS_CSV"]) if row["day"] == ds]


def transform_orders(ti, **_):
    return revenue(ti.xcom_pull(task_ids="extract"))


def load_revenue(ds, ti, **_):
    summary = ti.xcom_pull(task_ids="transform")
    payload = {"day": ds, "paid_orders": summary["paid_orders"], "cents": summary["cents"]}
    path = Path(os.environ["REVENUE_DIR"]) / "airflow.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return payload


with DAG(
    dag_id="daily_order_revenue",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=1)},
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_orders)
    transform = PythonOperator(task_id="transform", python_callable=transform_orders)
    load = PythonOperator(task_id="load", python_callable=load_revenue)
    extract >> transform >> load
