from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def run_ml(as_of):
    from ml.train_score import run_ml as train_and_score

    conn = PostgresHook(postgres_conn_id="deals_bigdata").get_conn()
    try:
        train_and_score(conn, as_of)
    finally:
        conn.close()


default_args = {
    "owner": "admin",
    "start_date": datetime(2026, 11, 9),
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="etl_prices",
    default_args=default_args,
    description="Dag to load prices into core, build route card mart and score ML predictions",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    render_template_as_native_obj=True,
    params={
        "start_date": Param(None, type=["null", "string"], format="date"),
        "end_date": Param(None, type=["null", "string"], format="date"),
        "as_of": Param("2026-10-26", type="string", format="date"),
    },
    tags=["etl", "mart", "ml"]
) as dag:
    start = EmptyOperator(task_id="start")

    load_core = SQLExecuteQueryOperator(
        task_id="load_core",
        conn_id="deals_bigdata",
        sql="etl_load.sql",
        parameters={
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
        },
        show_return_value_in_logs=True
    )

    build_mart = SQLExecuteQueryOperator(
        task_id="build_mart",
        conn_id="deals_bigdata",
        sql="mart_route_card.sql",
        parameters={"as_of": "{{ params.as_of }}"},
        show_return_value_in_logs=True
    )

    score_ml = PythonOperator(
        task_id="score_ml",
        python_callable=run_ml,
        op_kwargs={"as_of": "{{ params.as_of }}"}
    )

    end = EmptyOperator(task_id="end")

    start >> load_core >> build_mart >> score_ml >> end
