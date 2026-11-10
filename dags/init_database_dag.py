from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "admin",
    "start_date": datetime(2026, 11, 9),
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="create_schema",
    default_args=default_args,
    description="Dag to create schema, tables, indexes on core layer",
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    tags=["create schema", "create tables", "create indexes", "core layer"]
) as dag:
    start = EmptyOperator(task_id="start")

    create_core_schema = SQLExecuteQueryOperator(
        task_id="create_core_schema",
        conn_id="deals_bigdata",
        sql="core_schema.sql"
    )

    create_mart_schema = SQLExecuteQueryOperator(
        task_id="create_mart_schema",
        conn_id="deals_bigdata",
        sql="mart_schema.sql"
    )

    end = EmptyOperator(task_id="end")

    start >> create_core_schema >> create_mart_schema >> end
