
INPUT_BUCKET = "s3a://s3-us-east-1.amazonaws.com/yellow_taxi_files/"
OUTPUT_BUCKET = "s3a://s3-us-east-1.amazonaws.com/prd_yellow_taxi_table/"

# Define os meses que você quer processar
MONTHS = ["01", "02", "03", "04", "05"]
YEAR = "2023"

RELEVANT_COLUMNS = [
    "vendorid",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

SPARK_CONFIG = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.sql.shuffle.partitions": "20",
    "spark.hadoop.fs.s3a.endpoint": "http://localhost:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
}


from pyspark.sql import SparkSession
import os
def get_spark_session(app_name="YellowTaxiPipeline"):
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .enableHiveSupport()
        .getOrCreate()
    )
