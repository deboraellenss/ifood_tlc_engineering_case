import pytest
from pyspark.sql import SparkSession
from tlc_pipeline.silver_layer import transform_data  # ajuste o nome da função

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[1]") \
        .getOrCreate()

def test_transform_data(spark):
    df = spark.createDataFrame([
        (1, 2),
        (2, 3)
    ], ["VendorID", "passenger_count"])

    result_df = transform_data(df)

    assert "total_amount" in result_df.columns
