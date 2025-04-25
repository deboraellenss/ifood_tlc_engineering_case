"""
Unit tests for the Silver Layer transformation pipeline.
"""

import pytest
from unittest.mock import patch, MagicMock

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

from tlc_pipeline.silver_layer import SilverLayerTransformer, run_pipeline


@pytest.fixture
def spark():
    """Create a local SparkSession for testing."""
    return SparkSession.builder \
        .appName("SilverLayerTest") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def sample_bronze_df(spark):
    """Create a sample bronze layer DataFrame for testing."""
    schema = StructType([
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
    
    data = [
        ("1", "2", "2023-01-15 12:30:45", "2023-01-15 12:45:30", "15.5", "2.0", "0.5", "3.0", "0.0", "2023", "01"),
        ("2", "1", "2023-01-15 13:10:00", "2023-01-15 13:25:15", "12.0", "1.5", "0.5", "2.5", "5.0", "2023", "01"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_casted_df(spark):
    """Create a sample DataFrame with properly casted columns."""
    schema = StructType([
        StructField("vendorid", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True)
    ])
    
    # Note: For timestamp values in a DataFrame fixture, we need to use actual timestamp objects
    from datetime import datetime
    pickup_time1 = datetime(2023, 1, 15, 12, 30, 45)
    dropoff_time1 = datetime(2023, 1, 15, 12, 45, 30)
    pickup_time2 = datetime(2023, 1, 15, 13, 10, 0)
    dropoff_time2 = datetime(2023, 1, 15, 13, 25, 15)
    
    data = [
        (1, 2, pickup_time1, dropoff_time1, 15