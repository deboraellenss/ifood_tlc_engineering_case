from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bronze_layer import load_bronze_data
from silver_layer import clean_data
from gold_layer import save_gold_data

from pyspark.sql import SparkSession


def run_pipeline():
    spark = SparkSession.builder.appName("YellowTaxiPipeline").getOrCreate()

    df_bronze = load_bronze_data(spark)
    df_silver = clean_data(df_bronze)  # inclui validação GE
    save_gold_data(df_silver)


with DAG(
    dag_id="yellow_taxi_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    default_args={"owner": "data_engineer"},
) as dag:

    run_etl = PythonOperator(
        task_id="run_yellow_taxi_pipeline", python_callable=run_pipeline
    )
