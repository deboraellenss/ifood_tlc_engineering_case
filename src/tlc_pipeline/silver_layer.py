# silver_layer.py

from pyspark.sql.functions import col
from config import RELEVANT_COLUMNS
from validate import validate_spark_dataframe
from pyspark.sql import SparkSession
from config import INPUT_BUCKET, get_spark_session

def clean_data():
    spark =  get_spark_session()

    df = spark.read.parquet(INPUT_BUCKET)

    df = df.withColumn("VendorID", col("vendorid").cast("int")) \
           .withColumn("passenger_count", col("passenger_count").cast("int")) \
           .withColumn("tpep_pickup_datetime", col("requestdatetime").cast("timestamp")) \
           .withColumn("tpep_dropoff_datetime", col("dropoff_datetime").cast("timestamp"))
        #    .withColumn("total_amount", col("total_amount").cast("float")) \

    # Renomeia as colunas
    df= df.filter(col("VendorID").isNotNull()) 
    df.show()
    df = df.select(
        "vendorid",
        "passenger_count",
        "requestdatetime",
        "dropoff_datetime")

    # existing_columns = [col for col in RELEVANT_COLUMNS if col in df.columns]

    # # Seleciona apenas as colunas que existem no DataFrame
    # df = df.select(existing_columns)
    df.show() 
    # # Validação com GE
    # df = validate_spark_dataframe(df, suite_name="taxi_suite")

    return df

clean_data()