from tlc_pipeline.config import get_spark_session
from tlc_pipeline.raw_ingestion.normalize_schema import read_and_standardize_parquets
from tlc_pipeline.utils.write_bucket import write_to_raw_bucket
from tlc_pipeline.validate import validate_spark_dataframe
from tlc_pipeline.utils.logging_config import setup_logging

def run_pipeline(input_path, output_path, column_mapping):
    """
    Run the complete TLC data pipeline.
    
    Args:
        input_path: Path to input Parquet files
        output_path: Path to write processed data
        column_mapping: Column name mapping dictionary
    """
    logger = setup_logging()
    logger.info("Starting TLC data pipeline")
    
    try:
        # Initialize Spark
        logger.info("Initializing Spark session")
        spark = get_spark_session()
        
        # Read and standardize data
        logger.info(f"Reading and standardizing data from {input_path}")
        df = read_and_standardize_parquets(spark, input_path, column_mapping)
        
        if df is None:
            logger.error("No valid data found. Pipeline stopped.")
            return False
        
        # Validate data
        logger.info("Validating data quality")
        validation_result = validate_spark_dataframe(df)
        
        if not validation_result.success:
            logger.warning("Data validation failed, but continuing with pipeline")
        
        # Write to raw bucket
        logger.info(f"Writing data to {output_path}")
        success = write_to_raw_bucket(df, output_path)
        
        if success:
            logger.info("Pipeline completed successfully")
        else:
            logger.error("Failed to write data")
            
        return success
        
    except Exception as e:
        logger.exception(f"Pipeline failed: {str(e)}")
        return False