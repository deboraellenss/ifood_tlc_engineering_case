CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi (
  VendorID INT,
  passenger_count INT,
  total_amount DECIMAL(10,2),
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://prd_yellow_taxi_table/';
