"""
DAG de Orquestração - Pipeline SELIC Banco Central.
Orquestra de forma sequencial a execução das camadas Bronze, Silver e Gold,
contando com políticas de retentativas em caso de falha de comunicação com a API.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from bronze.ingestion import ingest_selic_data
from silver.transformation import transform_selic_data
from gold.aggregation import aggregate_selic_data

default_args = {
    "owner": "Emilio Gabriel",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,  
    "retry_delay": timedelta(minutes=2),  
}

with DAG(
    dag_id="pipeline_selic_bcb",
    default_args=default_args,
    description="Pipeline Medallion (Bronze-Silver-Gold) para dados da taxa SELIC",
    schedule_interval="@daily", 
    catchup=False,  
    tags=["beAnalytic", "selic", "bcb"],
) as dag:

    bronze_task = PythonOperator(
        task_id="ingestao_bronze",
        python_callable=ingest_selic_data,
    )

    silver_task = PythonOperator(
        task_id="transformacao_silver",
        python_callable=transform_selic_data,
    )

    gold_task = PythonOperator(
        task_id="agregacao_gold",
        python_callable=aggregate_selic_data,
    )

    bronze_task >> silver_task >> gold_task