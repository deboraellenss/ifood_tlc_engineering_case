from functools import reduce
from pyspark.sql.functions import col, lit, coalesce
from validate import validate_spark_dataframe
from config import INPUT_BUCKET, OUTPUT_BUCKET
from utils.helpers import get_spark_session, write_to_bucket



def clean_data():
    spark = get_spark_session()

    df = spark.read.parquet(f"{INPUT_BUCKET}/source_type=yellow/")
    
    df = df.withColumn("vendorid", col("vendorid").cast("int")) \
       .withColumn("passenger_count", col("passenger_count").cast("int")) \
       .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
       .withColumn("tpep_dropoff_datetime", col("dropoff_datetime").cast("timestamp"))\
       .withColumn("fare_amount", col("fare_amount").cast("float")) \
       .withColumn("extra", col("extra").cast("float")) \
       .withColumn("mta_tax", col("mta_tax").cast("float")) \
       .withColumn("tip_amount", col("tip_amount").cast("float")) \
       .withColumn("tolls_amount", col("tolls").cast("float")) 
       
    colunas_valores = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls"]

        # Expressão que soma todas essas colunas, tratando nulls como 0
    soma_total = reduce(lambda acc, c: acc + coalesce(col(c), lit(0.0)), colunas_valores[1:], coalesce(col(colunas_valores[0]), lit(0.0)))

        # Adiciona a coluna total_amount
    df = df.withColumn("total_amount", soma_total)\
        .select(
            "year", "month",
            "vendorid",
            "passenger_count",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "total_amount"
        )


    df.show()

    # Validação com GE
    df = validate_spark_dataframe(df, suite_name="taxi_suite_silver_layer")

    return df


df= clean_data()
write_to_bucket(df, OUTPUT_BUCKET,partition_columns=["year", "month"])

