from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import os

#Caminho e conexão
DATA_PATH = "/opt/airflow/data"
POSTGRES_CONN = "postgresql://airflow:airflow@postgres:5432/airflow"


# Tabelas
FILES_TO_TABLES = {
    "olist_orders_dataset.csv": "olist_orders_dataset",
    "olist_customers_dataset.csv": "olist_customers_dataset",
    "olist_order_items_dataset.csv": "olist_order_items_dataset",
    "olist_products_dataset.csv": "olist_products_dataset",
    "olist_sellers_dataset.csv": "olist_sellers_dataset",
    "olist_order_payments_dataset.csv": "olist_order_payments_dataset",
    "olist_order_reviews_dataset.csv": "olist_order_reviews_dataset",
    "product_category_name_translation.csv": "product_category_name_translation",
    "olist_geolocation_dataset.csv": "olist_geolocation_dataset"
}


def load_csv_to_bronze(file_name, table_name):
    """Carrega CSV no schema bronze (cria se não existir, senão faz truncate e reload)"""
    file_path = os.path.join(DATA_PATH, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo {file_name} não encontrado em {DATA_PATH}")

    df = pd.read_csv(file_path)

    engine = create_engine(POSTGRES_CONN)
    with engine.begin() as conn: 
       
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

        
        exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'bronze' AND table_name = :tabela
            )
        """), {"tabela": table_name}).scalar()

        if exists:
           
            conn.execute(text(f"TRUNCATE TABLE bronze.{table_name};"))
            df.to_sql(table_name, con=conn, schema="bronze", if_exists="append", index=False)
        else:
            
            df.to_sql(table_name, con=conn, schema="bronze", if_exists="replace", index=False)


# Configuração da DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "ingestao_bronze",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "ingestao"],
) as dag:

    for file_name, table_name in FILES_TO_TABLES.items():
        PythonOperator(
            task_id=f"load_{table_name}_to_bronze",
            python_callable=load_csv_to_bronze,
            op_kwargs={"file_name": file_name, "table_name": table_name},
        )
