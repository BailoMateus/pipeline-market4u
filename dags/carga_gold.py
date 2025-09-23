from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "carga_gold",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["gold", "processamento"],
    template_searchpath=["/opt/airflow/sql"], 
) as dag:

    dm_vendas_clientes = PostgresOperator(
        task_id="dm_vendas_clientes",
        postgres_conn_id="airflow_db",
        sql="gold/make_dm_vendas.sql",
    )
