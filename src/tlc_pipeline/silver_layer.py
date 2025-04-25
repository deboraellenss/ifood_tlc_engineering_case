import logging
from functools import reduce
from typing import Optional, List, Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, StringType
from pyspark.sql.utils import AnalysisException, IllegalArgumentException

from validate import validate_spark_dataframe
from config import INPUT_BUCKET, OUTPUT_BUCKET
from utils.helpers import get_spark_session, write_to_bucket, setup_logging



logger = setup_logging()


class SilverLayerTransformer:
    """
        A transformer class for processing raw taxi data into a standardized silver layer format.
        
        This class handles the transformation of raw taxi data through several key steps:
        - Reading source data from a specified input path
        - Casting columns to appropriate data types
        - Calculating total amount from individual amount columns
        - Selecting and ordering output columns
        - Validating the transformed data using Great Expectations
        
        Attributes:
            EXPECTED_SCHEMA (StructType): Defines the expected input schema for type safety
            COLUMN_MAPPING (dict): Maps source column names to standardized names
            OUTPUT_COLUMNS (list): Specifies the columns to include in the final output
            AMOUNT_COLUMNS (list): Columns used in total amount calculation
        
        Methods:
            transform(): Executes the full transformation pipeline for taxi data
        """

    
    # Schema definition for better type safety and documentation
    EXPECTED_SCHEMA = StructType([
        StructField("vendorid", StringType(), True),
        StructField("passenger_count", StringType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("fare_amount", StringType(), True),
        StructField("extra", StringType(), True),
        StructField("mta_tax", StringType(), True),
        StructField("tip_amount", StringType(), True),
        StructField("tolls", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True)
    ])
    
    # Column mapping for consistent naming
    COLUMN_MAPPING = {
        "dropoff_datetime": "tpep_dropoff_datetime",
        "tolls": "tolls_amount"
    }
    
    # Columns to include in final output
    OUTPUT_COLUMNS = [
        "year", "month", "vendorid", "passenger_count", 
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount"
    ]
    
    # Columns to use for total amount calculation
    AMOUNT_COLUMNS = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount"]
    
    def __init__(self, spark: SparkSession = None):
        """Initialize the transformer with a Spark session."""
        self.spark = spark or get_spark_session()
        logger.info("SilverLayerTransformer initialized")
    
    def read_source_data(self, input_path: str) -> Optional[DataFrame]:
        """Read source data from the specified path with error handling."""
        try:
            logger.info(f"Reading data from {input_path}")
            df = self.spark.read.parquet(input_path)
            
            row_count = df.count()
            logger.info(f"Read {row_count} rows from source")
            
            if row_count == 0:
                logger.warning("Source data is empty")
                return None
                
            return df
        except AnalysisException as e:
            logger.error(f"Failed to read source data: {str(e)}")
            raise
    
    def cast_columns(self, df: DataFrame) -> DataFrame:
        """Cast columns to appropriate data types."""
        logger.info("Casting columns to appropriate types")
        try:
            # Apply type casting
            casted_df = (
                df.withColumn("vendorid", col("vendorid").cast(IntegerType()))
                .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
                .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType()))
                .withColumn("tpep_dropoff_datetime", col("dropoff_datetime").cast(TimestampType()))
                .withColumn("fare_amount", col("fare_amount").cast(FloatType()))
                .withColumn("extra", col("extra").cast(FloatType()))
                .withColumn("mta_tax", col("mta_tax").cast(FloatType()))
                .withColumn("tip_amount", col("tip_amount").cast(FloatType()))
                .withColumn("tolls_amount", col("tolls").cast(FloatType()))
            )
            
            # Log schema after casting for debugging
            logger.debug("Schema after casting: %s", casted_df.schema.simpleString())
            return casted_df
            
        except IllegalArgumentException as e:
            logger.error(f"Error during column casting: {str(e)}")
            raise
    
    def calculate_total_amount(self, df: DataFrame) -> DataFrame:
        """Calculate total amount from individual amount columns."""
        logger.info("Calculating total amount")
        try:
            # Ensure all amount columns exist
            for col_name in self.AMOUNT_COLUMNS:
                if col_name not in df.columns:
                    logger.warning(f"Column {col_name} not found in dataframe, using 0.0")
            
            # Calculate total using reduce with null handling
            total_expr = reduce(
                lambda acc, col_name: acc + coalesce(col(col_name), lit(0.0)),
                self.AMOUNT_COLUMNS[1:],
                coalesce(col(self.AMOUNT_COLUMNS[0]), lit(0.0))
            )
            
            return df.withColumn("total_amount", total_expr)
        except Exception as e:
            logger.error(f"Error calculating total amount: {str(e)}")
            raise
    
    def select_output_columns(self, df: DataFrame) -> DataFrame:
        """Select and order columns for output."""
        logger.info(f"Selecting output columns: {', '.join(self.OUTPUT_COLUMNS)}")
        return df.select(*self.OUTPUT_COLUMNS)
    
    def validate_data(self, df: DataFrame, suite_name: str = "taxi_suite_silver_layer") -> DataFrame:
        """
        Validate the dataframe using Great Expectations.
        
        Logs a warning if validation fails but allows the pipeline to continue.
        
        Args:
            df: The DataFrame to validate
            suite_name: Name of the Great Expectations validation suite
            
        Returns:
            The original DataFrame, regardless of validation result
        """
        logger.info(f"Validating dataframe with suite: {suite_name}")
        try:
            validated_df = validate_spark_dataframe(df, suite_name=suite_name)
            logger.info("Data validation passed successfully")
            return validated_df
        except Exception as e:
            # Log a warning instead of an error, and continue processing
            logger.warning(f"Data validation failed: {str(e)}")
            logger.warning("Continuing pipeline execution despite validation failure")
            
            return df
    
    def transform(self) -> Optional[DataFrame]:
        """Execute the full transformation pipeline."""
        logger.info("Starting silver layer transformation")
        try:
            # Read source data
            input_path = f"{INPUT_BUCKET}/source_type=yellow/"
            df = self.read_source_data(input_path)
            if df is None:
                return None
            
            # Apply transformations
            df = self.cast_columns(df)
            df = self.calculate_total_amount(df)
            df = self.select_output_columns(df)
            
            # Preview data
            logger.info("Preview of transformed data:")
            df.show(5, truncate=False)
            
            # Validate
            df = self.validate_data(df)
            
            logger.info("Silver layer transformation completed successfully")
            return df
            
        except Exception as e:
            logger.error(f"Silver layer transformation failed: {str(e)}")
            raise


def run_pipeline():
    logger.info("Starting silver layer pipeline")
    try:
        transformer = SilverLayerTransformer()
        result_df = transformer.transform()
        
        if result_df is not None and not result_df.rdd.isEmpty():
            logger.info(f"Writing data to {OUTPUT_BUCKET}")
            write_to_bucket(
                result_df, 
                OUTPUT_BUCKET, 
                partition_columns=["year", "month"]
            )
            logger.info("Silver layer pipeline completed successfully")
            return True
        else:
            logger.warning("No data to write to output bucket")
            return False
            
    except Exception as e:
        logger.error(f"Silver layer pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()

