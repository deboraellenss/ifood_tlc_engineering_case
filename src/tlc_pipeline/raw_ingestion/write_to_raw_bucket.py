from pyspark.sql.functions import col


def write_to_raw_bucket(df, output_path):
    """
    Write DataFrame to raw bucket with proper partitioning.

    Args:
        df: Spark DataFrame to write
        output_path: S3 or local path to write data

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if df is None or df.isEmpty():
            print("No valid data to write.")
            return False

        df = df.withColumn("year", col("tpep_pickup_datetime").substr(1, 4)).withColumn(
            "month", col("tpep_pickup_datetime").substr(6, 2)
        )

        df = df.repartition(20, "year", "month")

        df.write.mode("overwrite").partitionBy("source_type", "year", "month").parquet(
            output_path
        )

        print("✅ Data written successfully!")
        return True
    except Exception as e:
        print(f"Error writing data to {output_path}: {str(e)}")
        return False
