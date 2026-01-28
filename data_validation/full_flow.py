import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# Create a DataContext as an entry point to the GX Python API
context = gx.get_context()

datasource_name = "nguyentn-nyctaxi"
my_connection_string = (
    "postgresql+psycopg2://k6:k6@localhost:5434/k6"
)

pg_datasource = context.sources.add_postgres(
    name=datasource_name, connection_string=my_connection_string
)

pg_datasource.add_table_asset(
    name="postgres_taxi_data", table_name="nyc_taxi", schema_name="staging"
)

batch_request = pg_datasource.get_asset("postgres_taxi_data").build_batch_request()


expectation_suite_name = "validate_trip_data"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

# Add the same expectations as the single-file
validator.expect_column_values_to_not_be_null("vendor_id")
validator.expect_column_values_to_not_be_null("rate_code_id")
validator.expect_column_values_to_not_be_null("dropoff_location_id")
validator.expect_column_values_to_not_be_null("pickup_location_id")
validator.expect_column_values_to_not_be_null("payment_type_id")
validator.expect_column_values_to_not_be_null("service_type")
validator.expect_column_values_to_not_be_null("pickup_latitude")
validator.expect_column_values_to_not_be_null("pickup_longitude")
validator.expect_column_values_to_not_be_null("dropoff_latitude")
validator.expect_column_values_to_not_be_null("dropoff_longitude")

validator.expect_column_values_to_be_between("trip_distance", min_value=0, max_value=100)
validator.expect_column_values_to_be_between("extra", min_value=0, max_value=3)

validator.save_expectation_suite(discard_failed_expectations=False)

# Similar to a single file, create a checkpoint to validate the result
# Define the checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="staging_tripdata_asset_checkpoint",
    validator=validator
)

# Get the result after validator
checkpoint_result = checkpoint.run()

# Quick view on the validation result
context.view_validation_result(checkpoint_result)