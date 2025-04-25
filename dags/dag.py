from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bronze_layer import load_bronze_data
from silver_layer import clean_data

from pyspark.sql import SparkSession


def run_bronze_layer():
    spark = SparkSession.builder.appName("YellowTaxiBronze").getOrCreate()
    load_bronze_data(spark)


def run_silver_layer():
    spark = SparkSession.builder.appName("YellowTaxiSilver").getOrCreate()
    df_bronze = load_bronze_data(spark)
    clean_data(df_bronze)


with DAG(
    dag_id="yellow_taxi_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    default_args={"owner": "data_engineer"},
) as dag:

    bronze_task = PythonOperator(
        task_id="run_bronze_layer",
        python_callable=run_bronze_layer
    )

    silver_task = PythonOperator(
        task_id="run_silver_layer",
        python_callable=run_silver_layer
    )

    bronze_task >> silver_task