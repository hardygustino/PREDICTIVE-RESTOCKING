from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"

default_args = {
    "owner": "hardy",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="restock_microbatch_hourly",
    default_args=default_args,
    description="Hourly micro-batch: Spark -> Inventory -> Restock -> Load to Postgres",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["restock", "microbatch", "olist"],
) as dag:

    # 1) Build fact_sales_daily
    t1_sales_daily = BashOperator(
        task_id="spark_sales_daily",
        bash_command=f"cd {PROJECT_DIR} && python spark/01_sales_daily.py",
    )

    # 2) Build inventory_snapshot
    t2_inventory = BashOperator(
        task_id="spark_inventory_snapshot",
        bash_command=f"cd {PROJECT_DIR} && python spark/02_inventory_snapshot.py",
    )

    # 3) Build restock_recommendation
    t3_restock = BashOperator(
        task_id="spark_restock_recommendation",
        bash_command=f"cd {PROJECT_DIR} && python spark/03_restock_recommendation.py",
    )

    # 4) Load into Postgres (restock-postgres di host)
    # Di dalam container, "localhost" adalah container itu sendiri, jadi pakai host.docker.internal
    t4_load_pg = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            "cd /opt/project && "
            "PGHOST=restock-postgres "
            "PGPORT=5432 "
            "PGDATABASE=restock_db "
            "PGUSER=restock "
            "PGPASSWORD=restock "
            "python sql/load_to_postgres.py"
        ),
    )


    t1_sales_daily >> t2_inventory >> t3_restock >> t4_load_pg
