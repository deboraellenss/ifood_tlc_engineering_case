# validate.py

import great_expectations as ge
from great_expectations.data_context import get_context
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
from config import INPUT_BUCKET, get_spark_session

def validate_spark_dataframe(df, suite_name="taxi_suite"):
    
    # Inicia contexto GE
    context = get_context()

    # Cria ou substitui a suite
    suite_name = "taxi_suite"
    suite = context.add_or_update_expectation_suite(suite_name)

    # Registra o datasource se necessário
    datasource_name = "spark_df_runtime"
    context.add_or_update_datasource(
        name=datasource_name,
        class_name="Datasource",
        execution_engine={"class_name": "SparkDFExecutionEngine"},
        data_connectors={
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"]
            }
        }
    )

    # Define o batch com o dataframe em tempo de execução
    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name="runtime_connector",
        data_asset_name="yellow_taxi_sample",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "yellow_taxi_batch"},
    )


    # Cria o Validator com o batch
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )

    # Expectativas principais
    validator.expect_column_to_exist("vendorid")
    validator.expect_column_values_to_not_be_null("vendorid")

    validator.expect_column_to_exist("passenger_count")
    validator.expect_column_values_to_not_be_null("passenger_count")
    validator.expect_column_values_to_be_between("passenger_count", 1, 8)

    validator.expect_column_values_to_not_be_null("fare_amount")
    validator.expect_column_values_to_be_between("fare_amount", min_value=0.01)

    validator.expect_column_values_to_not_be_null("requestdatetime")
    validator.expect_column_values_to_not_be_null("dropoff_datetime")

    validator.expect_column_values_to_be_between(
        "requestdatetime", min_value="2023-01-01", max_value="2023-05-31"
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    

    print("✅ Suite 'taxi_suite' criada com sucesso!")

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark =  get_spark_session()

    df_sample = spark.read.parquet(INPUT_BUCKET).limit(10000)
    print(df_sample.columns)
    df_sample.createOrReplaceTempView("sample_df")

    validate_spark_dataframe(df_sample)
