from pyspark.sql.functions import col

def write_to_raw_bucket(df, output_path):
    if df is not None:
        df = df.withColumn("year", col("requestdatetime").substr(1, 4)) \
               .withColumn("month", col("requestdatetime").substr(6, 2))
        
        df = df.repartition(20, "year", "month")

        df.write \
                .mode("overwrite") \
                .partitionBy("year", "month") \
                .parquet(output_path)
        print("✅ Dados escritos com sucesso!")
    else:
        print("Nenhum dado válido para escrever.")
