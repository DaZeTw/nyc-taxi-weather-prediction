import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# Create a DataContext as an entry point to the GX Python API
context = gx.get_context()

###############################################
# PostgreSQL Connection Configuration
###############################################
datasource_name = "nyc_taxi_weather_datasource"
my_connection_string = (
    "postgresql+psycopg2://k6:k6@localhost:5434/k6"
)

# Try to get existing datasource or create new one
try:
    pg_datasource = context.get_datasource(datasource_name)
    print(f"✅ Using existing datasource: {datasource_name}")
except:
    # Add PostgreSQL datasource if it doesn't exist
    pg_datasource = context.sources.add_postgres(
        name=datasource_name, 
        connection_string=my_connection_string
    )
    print(f"✅ Created new datasource: {datasource_name}")

# Add table asset (will skip if already exists)
try:
    asset = pg_datasource.get_asset("nyc_taxi_weather_asset")
    print("✅ Using existing table asset")
except:
    pg_datasource.add_table_asset(
        name="nyc_taxi_weather_asset", 
        table_name="nyc_taxi_weather", 
        schema_name="staging"
    )
    print("✅ Created new table asset")

# Build batch request
batch_request = pg_datasource.get_asset("nyc_taxi_weather_asset").build_batch_request()

###############################################
# Create Expectation Suite
###############################################
expectation_suite_name = "validate_nyc_taxi_weather"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

# Preview data
print("=" * 80)
print("📊 Data Preview:")
print("=" * 80)
print(validator.head())

###############################################
# Define Expectations
###############################################

print("\n" + "=" * 80)
print("🔍 Adding Data Quality Expectations...")
print("=" * 80)

# ==========================================
# 1. NULL Checks - Critical Columns
# ==========================================
critical_columns = [
    "taxi_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
    "pu_location_id", "do_location_id", "trip_distance", "passenger_count",
    "payment_type", "total_amount", "temperature_2m", "precipitation"
]

for col in critical_columns:
    validator.expect_column_values_to_not_be_null(col)

# ==========================================
# 2. Range Checks
# ==========================================
# Trip distance: positive values
validator.expect_column_values_to_be_between("trip_distance", min_value=0, max_value=100)

# Passenger count: 1-6
validator.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=6)

# Trip duration: reasonable range
validator.expect_column_values_to_be_between("trip_duration_minutes", min_value=0, max_value=300)

# Fare amount: positive
validator.expect_column_values_to_be_between("fare_amount", min_value=0, max_value=1000)

# Total amount: positive
validator.expect_column_values_to_be_between("total_amount", min_value=0, max_value=1000)

# Temperature: NYC range
validator.expect_column_values_to_be_between("temperature_2m", min_value=-30, max_value=120)

# Precipitation: 0-10 inches
validator.expect_column_values_to_be_between("precipitation", min_value=0, max_value=10)

# Wind speed: 0-150 mph
validator.expect_column_values_to_be_between("windspeed_10m", min_value=0, max_value=150)

# Hour: 0-23
validator.expect_column_values_to_be_between("hour_of_day", min_value=0, max_value=23)

# Day of week: 0-6
validator.expect_column_values_to_be_between("day_of_week", min_value=0, max_value=6)

# ==========================================
# 3. Categorical Checks (Simple)
# ==========================================
# Taxi type
validator.expect_column_values_to_be_in_set("taxi_type", value_set=["yellow", "green"])

# Rate code
validator.expect_column_values_to_be_in_set("rate_code_id", value_set=[1, 2, 3, 4, 5, 6])

# Payment type
validator.expect_column_values_to_be_in_set("payment_type", value_set=[1, 2, 3, 4, 5, 6])

# Store and forward flag
validator.expect_column_values_to_be_in_set("store_and_fwd_flag", value_set=["Y", "N"])

# ==========================================
# 4. Logical Checks
# ==========================================
# Dropoff after pickup (FIX: use column_A and column_B)
validator.expect_column_pair_values_a_to_be_greater_than_b(
    column_A="dropoff_datetime",
    column_B="pickup_datetime"
)

# Total >= Fare (FIX: use column_A and column_B)
validator.expect_column_pair_values_a_to_be_greater_than_b(
    column_A="total_amount",
    column_B="fare_amount",
    or_equal=True
)

# Table has data
validator.expect_table_row_count_to_be_between(min_value=1)

# ==========================================
# Save Expectation Suite
# ==========================================
print("\n" + "=" * 80)
print("💾 Saving Expectation Suite...")
print("=" * 80)

validator.save_expectation_suite(discard_failed_expectations=False)

###############################################
# Create and Run Checkpoint
###############################################
print("\n" + "=" * 80)
print("🚀 Running Validation...")
print("=" * 80)

checkpoint = context.add_or_update_checkpoint(
    name="staging_nyc_taxi_weather_checkpoint",
    validator=validator
)

# Run validation
checkpoint_result = checkpoint.run()

# Print summary
print("\n" + "=" * 80)
print("📊 Validation Results:")
print("=" * 80)

if checkpoint_result["success"]:
    print("\n✅ All validations passed!")
else:
    print("\n⚠️  Some validations failed. Review the details above.")
    
# Optional: View detailed results
context.view_validation_result(checkpoint_result)