from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import os

SQL_PATH = "/opt/airflow/sql/silver"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "processamento_silver",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["silver", "processamento"],
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    customers = PostgresOperator(
        task_id="customers_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/customers_to_silver.sql",
    )

    geolocation = PostgresOperator(
        task_id="geolocation_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/geolocation_to_silver.sql",
    )

    order_items = PostgresOperator(
        task_id="order_items_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/order_items_to_silver.sql",
    )

    order_payments = PostgresOperator(
        task_id="order_payments_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/order_payments_to_silver.sql",
    )

    order_reviews = PostgresOperator(
        task_id="order_reviews_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/order_reviews_to_silver.sql",
    )

    orders = PostgresOperator(
        task_id="orders_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/orders_to_silver.sql",
    )

    products = PostgresOperator(
        task_id="products_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/products_to_silver.sql",
    )

    sellers = PostgresOperator(
        task_id="sellers_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/sellers_to_silver.sql",
    )

    translation = PostgresOperator(
        task_id="translation_to_silver",
        postgres_conn_id="airflow_db",
        sql="silver/product_category_name_translation_to_silver.sql",
    )

    [customers, geolocation, order_items, order_payments, order_reviews,
     orders, products, sellers, translation]
