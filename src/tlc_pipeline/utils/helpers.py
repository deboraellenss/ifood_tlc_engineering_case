from pyspark.sql import SparkSession
import os
import logging

def get_spark_session(app_name: str = "TaxiDataPipeline", config_overrides: dict | None = None) -> SparkSession:
    """
    Create and configure a Spark session.

    Args:
        app_name: Name of the Spark application
        config_overrides: Dict of config parameters to override defaults

    Returns:
        SparkSession: Configured Spark session
    """
    # Get configuration from environment or use defaults
 
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    # Default configuration
    default_config: dict[str, str] = {
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.sql.shuffle.partitions": "20",
        "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
        "spark.hadoop.fs.s3a.access.key": minio_access_key,
        "spark.hadoop.fs.s3a.secret.key": minio_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901",
    }

    # Override defaults with provided config
    if config_overrides:
        default_config.update(config_overrides)

    # Build session with all configs
    builder = SparkSession.builder.appName(app_name)
    for key, value in default_config.items():
        builder = builder.config(key, value)

    return builder.enableHiveSupport().getOrCreate()


def write_to_bucket(df, output_path: str, partition_columns: list[str]):    
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

        # df = df.repartition(20, "year", "month")

        df.write.mode("overwrite").partitionBy(partition_columns).parquet(output_path)

        print("✅ Data written successfully!")
        return True
    except Exception as e:
        print(f"Error writing data to {output_path}: {str(e)}")
        return False



def setup_logging(log_level=logging.INFO):
    """
        Configure and set up logging for the TLC pipeline application.
        
        Args:
            log_level (int, optional): Logging level to set. Defaults to logging.INFO.
        
        Returns:
            logging.Logger: Configured logger for the 'tlc_pipeline' application.
        """
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("tlc_pipeline")
