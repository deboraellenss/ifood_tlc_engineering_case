import os
import great_expectations as ge
from great_expectations.data_context import get_context
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.validator.validator import Validator
from config import INPUT_BUCKET
from utils.helpers import get_spark_session


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
                "batch_identifiers": ["default_identifier_name"],
            }
        },
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
        batch_request=batch_request, expectation_suite=suite
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

    # Step 4: Create (or load) a validator and build an expectation suite from full profiling
    profiler = UserConfigurableProfiler(profile_dataset=validator)
    suite = profiler.build_suite()

    # Step 5: Save the full profiled expectation suite
    context.save_expectation_suite(
        expectation_suite=suite, expectation_suite_name=suite_name
    )

    # Step 6: Run validation with the new expectations using a Checkpoint
    checkpoint = SimpleCheckpoint(
        name=f"{suite_name}_temp_checkpoint",
        data_context=context,
        validator=validator,
    )

    result = checkpoint.run()

    if not result["success"]:
        raise Exception("Great Expectations validation failed.")

    # Step 7: Build data docs (HTML report)
    context.build_data_docs()

    # Step 8: Return path to generated HTML report
    docs_path = os.path.abspath(
        "great_expectations/uncommitted/data_docs/local_site/index.html"
    )
    print(f"📊 Full profiling report saved at: {docs_path}")

    print("✅ Suite 'taxi_suite' criada com sucesso!")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = get_spark_session()

    df_sample = spark.read.parquet(INPUT_BUCKET).limit(10000)
    # df_sample=df_sample.filter(df_sample.vendorid.isNotNull()).sample(0.01)

    df_sample.createOrReplaceTempView("sample_df")

    validate_spark_dataframe(df_sample)
