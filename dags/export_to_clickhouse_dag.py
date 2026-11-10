from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

default_args = {
    "owner": "admin",
    "start_date": datetime(2026, 11, 9),
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="export_to_clickhouse",
    default_args=default_args,
    description="Dag to export data into clickhouse mart layer",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/sql"],
    tags=["clickhouse", "export", "mart", "ml"]
) as dag:
    start = EmptyOperator(task_id="start")

    export_route_card = ClickHouseOperator(
        task_id="export_route_card",
        clickhouse_conn_id="clickhouse_default",
        sql="clickhouse_route_card.sql"
    )

    export_predictions = ClickHouseOperator(
        task_id="export_predictions",
        clickhouse_conn_id="clickhouse_default",
        sql="clickhouse_ml_price_predictions.sql"
    )

    end = EmptyOperator(task_id="end")

    start >> export_route_card >> export_predictions >> end
