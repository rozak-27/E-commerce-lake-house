"""
Daily Lakehouse Pipeline DAG
=============================
Jadwal: setiap jam
Urutan: Silver Clean → Gold Aggregate

Pakai BashOperator — tidak perlu install provider tambahan.
Airflow trigger spark-submit langsung via docker exec.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SPARK_SUBMIT = (
    "docker exec spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="daily_lakehouse_pipeline",
    description="Silver Clean → Gold Aggregate, setiap jam",
    default_args=default_args,
    start_date=datetime(2026, 5, 25),
    schedule_interval="@hourly",
    catchup=False,
    tags=["daily", "ecommerce", "lakehouse", "data-engineering"],
) as dag:

    # ── Task 1: Silver Clean ──────────────────────────────
    silver_clean = BashOperator(
        task_id="silver_clean",
        bash_command=SPARK_SUBMIT + "/opt/spark/work-dir/silver_clean.py",
        execution_timeout=timedelta(minutes=30),
    )

    # ── Task 2: Gold Aggregate ────────────────────────────
    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command=SPARK_SUBMIT + "/opt/spark/work-dir/gold_aggregate.py",
        execution_timeout=timedelta(minutes=30),
    )

    # ── Task 3: Notify ────────────────────────────────────
    def notify_done(**context):
        print(f"✅ Pipeline selesai: {context['execution_date']}")
        print("   Silver + Gold sudah diupdate!")

    notify = PythonOperator(
        task_id="notify_done",
        python_callable=notify_done,
    )

    # ── Urutan ────────────────────────────────────────────
    silver_clean >> gold_aggregate >> notify