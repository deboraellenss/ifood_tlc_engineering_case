
from pyspark.sql import SparkSession
import os


def get_spark_session(app_name="TaxiDataPipeline", config_overrides=None):
    """
    Create and configure a Spark session.
    
    Args:
        app_name: Name of the Spark application
        config_overrides: Dict of config parameters to override defaults
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Get configuration from environment or use defaults
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    
    # Default configuration
    default_config = {
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.sql.shuffle.partitions": "20",
        "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
        "spark.hadoop.fs.s3a.access.key": minio_access_key,
        "spark.hadoop.fs.s3a.secret.key": minio_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901"
    }
    
    # Override defaults with provided config
    if config_overrides:
        default_config.update(config_overrides)
    
    # Build session with all configs
    builder = SparkSession.builder.appName(app_name)
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    return builder.enableHiveSupport().getOrCreate()
