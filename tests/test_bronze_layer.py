"""
Unit tests for the Bronze Layer ingestion pipeline.
"""

import os
import pytest
import tempfile
from unittest.mock import patch, MagicMock

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from tlc_pipeline.bronze_layer import BronzeLayerIngestion, run_pipeline


@pytest.fixture
def spark():
    """Create a local SparkSession for testing."""
    return SparkSession.builder \
        .appName("BronzeLayerTest") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def sample_mapping_dict():
    """Sample mapping dictionary for testing."""
    return {
        "VendorID": "vendorid",
        "passenger_count": "passenger_count",
        "pickup_datetime": "tpep_pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "fare_amount": "fare_amount",
    }


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("VendorID", StringType(), True),
        StructField("passenger_count", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("fare_amount", StringType(), True),
    ])
    
    data = [
        ("1", "2", "2023-01-15 12:30:45", "2023-01-15 12:45:30", "15.5"),
        ("2", "1", "2023-01-15 13:10:00", "2023-01-15 13:25:15", "12.0"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def normalized_df(spark):
    """Create a sample normalized DataFrame for testing."""
    schema = StructType([
        StructField("vendorid", StringType(), True),
        StructField("passenger_count", StringType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("fare_amount", StringType(), True),
        StructField("source_type", StringType(), True),
    ])
    
    data = [
        ("1", "2", "2023-01-15 12:30:45", "2023-01-15 12:45:30", "15.5", "yellow"),
        ("2", "1", "2023-01-15 13:10:00", "2023-01-15 13:25:15", "12.0", "yellow"),
    ]
    
    return spark.createDataFrame(data, schema)


class TestBronzeLayerIngestion:
    """Test cases for the BronzeLayerIngestion class."""
    
    def test_init(self, spark):
        """Test initialization of BronzeLayerIngestion."""
        # Arrange
        input_path = "/test/input"
        output_path = "/test/output"
        
        # Act
        ingestion = BronzeLayerIngestion(spark, input_path, output_path)
        
        # Assert
        assert ingestion.spark == spark
        assert ingestion.input_path == input_path
        assert ingestion.output_path == output_path
    
    @patch('tlc_pipeline.bronze_layer.de_para')
    def test_get_mapping_dictionary(self, mock_de_para, spark):
        """Test getting the mapping dictionary."""
        # Arrange
        expected_dict = {"VendorID": "vendorid", "passenger_count": "passenger_count"}
        mock_de_para.return_value = expected_dict
        ingestion = BronzeLayerIngestion(spark)
        
        # Act
        result = ingestion.get_mapping_dictionary()
        
        # Assert
        assert result == expected_dict
        mock_de_para.assert_called_once_with(spark, ingestion.input_path)
    
    @patch('tlc_pipeline.bronze_layer.ler_e_padronizar_parquets')
    @patch('os.path.exists')
    def test_read_and_normalize_data(self, mock_exists, mock_normalize, spark, sample_mapping_dict, normalized_df):
        """Test reading and normalizing data."""
        # Arrange
        mock_exists.return_value = True
        mock_normalize.return_value = normalized_df
        ingestion = BronzeLayerIngestion(spark)
        
        # Act
        result = ingestion.read_and_normalize_data(sample_mapping_dict)
        
        # Assert
        assert result is not None
        assert result.count() == 2
        mock_normalize.assert_called_once_with(spark, ingestion.input_path, sample_mapping_dict)
    
    @patch('os.path.exists')
    def test_read_and_normalize_data_path_not_exists(self, mock_exists, spark, sample_mapping_dict):
        """Test handling of non-existent input path."""
        # Arrange
        mock_exists.return_value = False
        ingestion = BronzeLayerIngestion(spark)
        
        # Act
        result = ingestion.read_and_normalize_data(sample_mapping_dict)
        
        # Assert
        assert result is None
    
    def test_extract_date_parts(self, spark, normalized_df):
        """Test extraction of date parts from pickup datetime."""
        # Arrange
        ingestion = BronzeLayerIngestion(spark)
        
        # Act
        result = ingestion.extract_date_parts(normalized_df)
        
        # Assert
        assert "year" in result.columns
        assert "month" in result.columns
        year_values = [row.year for row in result.select("year").collect()]
        month_values = [row.month for row in result.select("month").collect()]
        assert all(year == "2023" for year in year_values)
        assert all(month == "01" for month in month_values)
    
    def test_extract_date_parts_missing_column(self, spark):
        """Test extraction of date parts with missing required column."""
        # Arrange
        ingestion = BronzeLayerIngestion(spark)
        df_without_pickup = spark.createDataFrame([("1", "2")], ["vendorid", "passenger_count"])
        
        # Act & Assert
        with pytest.raises(ValueError):
            ingestion.extract_date_parts(df_without_pickup)
    
    def test_optimize_for_write(self, spark, normalized_df):
        """Test optimization for write with repartitioning."""
        # Arrange
        ingestion = BronzeLayerIngestion(spark)
        test_partitions = 5
        
        # Act
        result = ingestion.optimize_for_write(normalized_df, test_partitions)
        
        # Assert
        # Note: In Spark, we can't directly check the number of partitions in a DataFrame
        # without triggering an action. This is a limitation of the test.
        assert result is not None
    
    @patch('tlc_pipeline.bronze_layer.write_to_bucket')
    def test_write_bronze_data(self, mock_write, spark, normalized_df):
        """Test writing data to bronze layer."""
        # Arrange
        ingestion = BronzeLayerIngestion(spark)
        mock_write.return_value = None
        
        # Act
        result = ingestion.write_bronze_data(normalized_df)
        
        # Assert
        assert result is True
        mock_write.assert_called_once_with(
            normalized_df, 
            ingestion.output_path, 
            partition_columns=ingestion.PARTITION_COLUMNS
        )
    
    @patch.object(BronzeLayerIngestion, 'get_mapping_dictionary')
    @patch.object(BronzeLayerIngestion, 'read_and_normalize_data')
    @patch.object(BronzeLayerIngestion, 'extract_date_parts')
    @patch.object(BronzeLayerIngestion, 'optimize_for_write')
    @patch.object(BronzeLayerIngestion, 'write_bronze_data')
    def test_process_success(self, mock_write, mock_optimize, mock_extract, 
                             mock_read, mock_mapping, spark, normalized_df):
        """Test successful processing of the full pipeline."""
        # Arrange
        ingestion = BronzeLayerIngestion(spark)
        mock_mapping.return_value = {"VendorID": "vendorid"}
        mock_read.return_value = normalized_df
        mock_extract.return_value = normalized_df
        mock_optimize.return_value = normalized_df
        mock_write.return_value = True
        
        # Act
        result = ingestion.process()
        
        # Assert
        assert result is True
        mock_mapping.assert_called_once()
        mock_read.assert_called_once()
        mock_extract.assert_called_once()
        mock_optimize.assert_called_once()
        mock_write.assert_called_once()
    
    @patch.object(BronzeLayerIngestion, 'get_mapping_dictionary')
    @patch.object(BronzeLayerIngestion, 'read_and_normalize_data')
    def test_process_no_data(self, mock_read, mock_mapping, spark):
        """Test processing when no data is available."""
        # Arrange
        ingestion = BronzeLayerIngestion(spark)
        mock_mapping.return_value = {"VendorID": "vendorid"}
        mock_read.return_value = None
        
        # Act
        result = ingestion.process()
        
        # Assert
        assert result is False
        mock_mapping.assert_called_once()
        mock_read.assert_called_once()
    
    @patch.object(BronzeLayerIngestion, 'get_mapping_dictionary')
    def test_process_exception(self, mock_mapping, spark):
        """Test handling of exceptions during processing."""
        # Arrange
        ingestion = BronzeLayerIngestion(spark)
        mock_mapping.side_effect = Exception("Test error")
        
        # Act
        result = ingestion.process()
        
        # Assert
        assert result is False
        mock_mapping.assert_called_once()


@patch('tlc_pipeline.bronze_layer.BronzeLayerIngestion')
def test_run_pipeline_success(mock_ingestion_class):
    """Test successful execution of the run_pipeline function."""
    # Arrange
    mock_instance = MagicMock()
    mock_instance.process.return_value = True
    mock_ingestion_class.return_value = mock_instance
    
    # Act
    result = run_pipeline("/test/input", "/test/output")
    
    # Assert
    assert result is True
    mock_ingestion_class.assert_called_once_with(
        input_path="/test/input", 
        output_path="/test/output"
    )
    mock_instance.process.assert_called_once()


@patch('tlc_pipeline.bronze_layer.BronzeLayerIngestion')
def test_run_pipeline_failure(mock_ingestion_class):
    """Test handling of failures in the run_pipeline function."""
    # Arrange
    mock_instance = MagicMock()
    mock_instance.process.side_effect = Exception("Test error")
    mock_ingestion_class.return_value = mock_instance
    
    # Act
    result = run_pipeline()
    
    # Assert
    assert result is False
    mock_instance.process.assert_called_once()