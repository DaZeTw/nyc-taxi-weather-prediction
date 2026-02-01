import sys
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType, TimestampType

# Setup Paths
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "config/datalake.yaml"

def get_spark_session(cfg, endpoint_url, access_key, secret_key):
    """
    Creates a Spark session configured for MinIO (S3A) with optimized memory settings.
    """
    return SparkSession.builder \
        .appName("NYC_Taxi_Spark_Transform") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint_url}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.files.maxRecordsPerFile", "100000") \
        .getOrCreate()

def check_null_percentage(df):
    """
    Check null percentage for each column and return columns to drop (>95% null)
    """
    total_count = df.count()
    print(f"\n📊 Checking null percentages (Total records: {total_count:,})")
    print("="*70)
    
    columns_to_drop = []
    
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
        
        status = "❌ DROP" if null_percentage > 95 else "✅ KEEP"
        print(f"{col_name:30} | Nulls: {null_count:>8,} ({null_percentage:>6.2f}%) | {status}")
        
        if null_percentage > 95:
            columns_to_drop.append(col_name)
    
    print("="*70)
    if columns_to_drop:
        print(f"⚠️  Columns to drop (>95% null): {', '.join(columns_to_drop)}")
    else:
        print("✅ No columns have >95% null values")
    print()
    
    return columns_to_drop

def process_taxi_data(df, taxi_type, target_schema):
    """
    Process taxi data with strict type casting
    Handles both Green and Yellow taxi schemas
    
    Args:
        df: Input DataFrame
        taxi_type: "green" or "yellow"
        target_schema: Dict of {column_name: spark_type}
    """
    print(f"📗 Processing {taxi_type.upper()} taxi data")
    
    # Lowercase all columns
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    
    # Add taxi_type column
    df = df.withColumn("taxi_type", F.lit(taxi_type))
    
    # ==========================================
    # Standardize datetime column names
    # ==========================================
    if taxi_type == "green":
        if "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        if "lpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    else:  # yellow
        if "tpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        if "tpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    
    # ==========================================
    # Standardize other column names
    # ==========================================
    column_mapping = {
        "vendorid": "vendor_id",
        "ratecodeid": "rate_code_id",
        "pulocationid": "pu_location_id",
        "dolocationid": "do_location_id"
    }
    
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    
    # ==========================================
    # Drop unwanted columns
    # ==========================================
    columns_to_drop_manual = [
        "ehail_fee", 
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee"
    ]
    
    for col in columns_to_drop_manual:
        if col in df.columns:
            df = df.drop(col)
            print(f"🗑️  Dropped column: {col}")
    
    # Check null percentages and get columns to drop
    columns_to_drop = check_null_percentage(df)
    
    # Drop columns with >95% null
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
        print(f"🗑️  Dropped {len(columns_to_drop)} columns with >95% null values")
    
    # ==========================================
    # Build final DataFrame with explicit type casting
    # ==========================================
    print("\n🔧 Casting columns to target schema...")
    
    final_select = []
    
    for col_name, col_type in target_schema.items():
        if col_name == "taxi_type":
            # Already added above
            final_select.append(F.col("taxi_type"))
        elif col_name in df.columns:
            # Cast existing column to target type
            final_select.append(F.col(col_name).cast(col_type).alias(col_name))
            print(f"  ✅ {col_name:25} → {col_type}")
        else:
            # Add missing column as null with correct type
            final_select.append(F.lit(None).cast(col_type).alias(col_name))
            print(f"  ⚠️  {col_name:25} → {col_type} (NULL - missing)")
    
    df = df.select(final_select)
    
    print(f"\n✅ Schema standardized: {len(target_schema)} columns")
    
    return df

def transform_data(endpoint_url, access_key, secret_key, years=[2024]):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    # Initialize Spark
    spark = get_spark_session(cfg, endpoint_url, access_key, secret_key)

    # Initialize MinIO Client for bucket creation
    client = MinIOClient(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)
    client.create_bucket(datalake_cfg['bucket_name_2'])

    # Output path
    output_path = f"s3a://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}/"
    
    # ==========================================
    # Define TARGET SCHEMA with explicit types
    # ==========================================
    target_schema = {
        "taxi_type": StringType(),
        "vendor_id": LongType(),
        "pickup_datetime": TimestampType(),
        "dropoff_datetime": TimestampType(),
        "passenger_count": LongType(),
        "trip_distance": DoubleType(),          # MUST be Double
        "rate_code_id": LongType(),
        "store_and_fwd_flag": StringType(),
        "pu_location_id": LongType(),
        "do_location_id": LongType(),
        "payment_type": LongType(),
        "fare_amount": DoubleType(),            # MUST be Double
        "extra": DoubleType(),                  # MUST be Double
        "mta_tax": DoubleType(),                # MUST be Double
        "tip_amount": DoubleType(),             # MUST be Double
        "tolls_amount": DoubleType(),           # MUST be Double
        "improvement_surcharge": DoubleType(),  # MUST be Double
        "total_amount": DoubleType()            # MUST be Double
    }
    
    print("\n📋 TARGET SCHEMA:")
    print("="*70)
    for col_name, col_type in target_schema.items():
        print(f"  {col_name:25} → {col_type}")
    print("="*70)
    
    # Track statistics
    total_processed = 0
    total_files = 0
    
    # ==========================================
    # Process each year and month separately
    # ==========================================
    for year in years:
        base_path = f"s3a://{datalake_cfg['bucket_name_1']}/{datalake_cfg['folder_name']}/"
        
        for month in range(1, 13):
            year_month = f"{year}-{month:02d}"
            
            # Try both green and yellow (adjust based on your data)
            file_paths = [
                (f"{base_path}green_tripdata_{year_month}.parquet", "green"),
                (f"{base_path}yellow_tripdata_{year_month}.parquet", "yellow")
            ]
            
            for file_path, taxi_type in file_paths:
                try:
                    print(f"\n{'='*100}")
                    print(f"📂 Processing: {year_month} - {taxi_type.upper()}")
                    print(f"📄 Reading: {file_path}")
                    
                    # Read monthly data
                    df = spark.read.parquet(file_path)
                    record_count = df.count()
                    print(f"✅ Loaded: {record_count:,} records")
                    
                    # Process the data with enforced schema
                    df = process_taxi_data(df, taxi_type, target_schema)
                    
                    # Add year and month columns BEFORE writing
                    df = df.withColumn("year", F.lit(int(year)).cast(IntegerType())) \
                           .withColumn("month", F.lit(month).cast(IntegerType()))
                    
                    # ==========================================
                    # Final verification of schema
                    # ==========================================
                    print(f"\n📋 Final Schema for {taxi_type} {year_month}:")
                    df.printSchema()
                    
                    final_count = df.count()
                    print(f"✅ Records to write: {final_count:,}")
                    
                    # ==========================================
                    # Write with partitioning - APPEND mode
                    # ==========================================
                    print(f"💾 Writing to: {output_path}")
                    print(f"📁 Partition: year={year}/month={month}")
                    
                    df.write \
                        .mode("append") \
                        .partitionBy("year", "month") \
                        .parquet(output_path)
                    
                    total_processed += final_count
                    total_files += 1
                    print(f"✅ Successfully wrote {taxi_type} {year_month}")
                    
                except Exception as e:
                    print(f"⚠️  Skipping {taxi_type} {year_month} - Error: {e}")
                    # Don't print full traceback for missing files
                    if "Path does not exist" not in str(e):
                        import traceback
                        traceback.print_exc()
                    continue
    
    # ==========================================
    # Final summary
    # ==========================================
    print(f"\n{'='*100}")
    print(f"🎉 Transformation completed!")
    print(f"📊 Total files processed: {total_files}")
    print(f"📊 Total records processed: {total_processed:,}")
    print(f"📂 Output location: {output_path}")
    
    # Read back and show summary (with error handling)
    try:
        print(f"\n📖 Reading back data for verification...")
        df_result = spark.read.parquet(output_path)
        
        print("\n📊 Final Schema:")
        df_result.printSchema()
        
        print(f"\n✅ Total records: {df_result.count():,}")
        
        print("\n🔍 Sample Data (first 5 rows):")
        df_result.show(5, truncate=False)
        
        print("\n📁 Data Distribution by Year-Month-TaxiType:")
        df_result.groupBy("year", "month", "taxi_type").count() \
            .orderBy("year", "month", "taxi_type").show(100)
        
        print("\n📊 Type verification for numeric columns:")
        for col_name in ["trip_distance", "fare_amount", "total_amount"]:
            print(f"  {col_name}: {dict(df_result.dtypes)[col_name]}")
        
    except Exception as e:
        print(f"⚠️  Could not read back full dataset for verification: {e}")
        import traceback
        traceback.print_exc()
    
    print("="*100)
    
    spark.stop()
    print("🛑 Spark session stopped")
    print("✅ All done!")

if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    ENDPOINT_URL_LOCAL = "minio:9000"
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']

    transform_data(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)