import os
from pyspark.sql import SparkSession
from raw_ingestion.normalize_schema import ler_e_padronizar_parquets
from raw_ingestion.write_to_raw_bucket import write_to_raw_bucket
from raw_ingestion.de_para import de_para


# Configurações para acessar o MinIO (simulando o S3)
minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

# Cria a SparkSession com suporte a S3 (MinIO)
spark = (
    SparkSession.builder.appName("MinIO com PySpark")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
    )
    .getOrCreate()
)


file_paths = "src/tlc_pipeline/data/tlc_trip_record_data/"
output_path = "s3a://s3-us-east-1.amazonaws.com/yellow_taxi_files/"

print("✅ Driver memory:", spark.sparkContext.getConf().get("spark.driver.memory"))
print("✅ Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))


if __name__ == "__main__":
    de_para_padrao = de_para(spark, file_paths)
    raw_df = ler_e_padronizar_parquets(spark, file_paths, de_para_padrao)
    write_to_raw_bucket(raw_df, output_path)
