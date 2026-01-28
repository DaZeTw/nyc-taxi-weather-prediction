import sys
import os
import logging
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "config/datalake.yaml"
YEARS = [2018]  # Process only 2018 for testing

###############################################

def get_spark_session(endpoint_url, access_key, secret_key):
    """
    Create Spark session
    """
    return SparkSession.builder \
        .appName("Validate_Data") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint_url}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.default.parallelism", "16") \
        .config("spark.sql.files.maxRecordsPerFile", "100000") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

def validate_data(endpoint_url, access_key, secret_key):
    """
    ⚡ OPTIMIZED: Validate data after weather merge with single-pass operations
    - Filter out records with null values in critical columns
    - Keep only complete records
    - Write validated data to bucket_2/batch_validated/
    """
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    
    logging.info("🔍 Starting data validation...")
    
    # Initialize Spark
    spark = get_spark_session(endpoint_url, access_key, secret_key)
    logging.info('✅ Spark session created')

    # Initialize MinIO client
    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    try:
        # INPUT: bucket_2 with weather (unvalidated)
        input_base = f"s3a://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}_with_weather/"
        
        # OUTPUT: bucket_2 validated (clean, no nulls)
        output_path = f"s3a://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}_validated/"
        
        logging.info(f"📥 Input (with weather): {input_base}")
        logging.info(f"📤 Output (validated): {output_path}")
        
        # Required columns that must not be null
        REQUIRED_COLUMNS = [
            "taxi_type",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "rate_code_id",
            "pu_location_id",
            "do_location_id",
            "temperature_2m",
            "precipitation",
            "windspeed_10m",
            "pressure_msl"
        ]
        
        logging.info(f"📋 Required columns (no nulls allowed): {REQUIRED_COLUMNS}")
        
        total_input = 0
        total_output = 0
        total_removed = 0
        validation_stats = []
        
        # ==========================================
        # Process each year-month separately
        # ==========================================
        for year in YEARS:
            for month in range(1, 13):
                print(f"\n{'='*100}")
                logging.info(f"📅 Processing: {year}-{month:02d}")
                
                try:
                    # Read parquet with weather
                    input_path = f"{input_base}year={year}/month={month}/"
                    
                    logging.info(f"📂 Reading: {input_path}")
                    df = spark.read.parquet(input_path)
                    
                    # Add partition columns back
                    df = df.withColumn("year", F.lit(year)) \
                           .withColumn("month", F.lit(month))
                    
                    # ==========================================
                    # ⚡ OPTIMIZATION: Single-pass null analysis
                    # ==========================================
                    logging.info("\n🔍 Analyzing data quality (single pass)...")
                    
                    # Build aggregation expressions for one-shot analysis
                    agg_exprs = [
                        F.count("*").alias("total_records")
                    ]
                    
                    # Add null counts for required columns
                    for col in REQUIRED_COLUMNS:
                        agg_exprs.append(
                            F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"null_{col}")
                        )
                    
                    # Execute single aggregation
                    stats = df.agg(*agg_exprs).collect()[0]
                    
                    input_count = stats["total_records"]
                    total_input += input_count
                    
                    logging.info(f"✅ Loaded {input_count:,} records")
                    logging.info("\n📊 Null value counts:")
                    
                    has_nulls = False
                    for col in REQUIRED_COLUMNS:
                        null_count = stats[f"null_{col}"]
                        if null_count > 0:
                            has_nulls = True
                            logging.info(f"   ⚠️  {col}: {null_count:,} nulls ({null_count/input_count*100:.2f}%)")
                    
                    if not has_nulls:
                        logging.info("   ✅ No null values found in required columns")
                    
                    # ==========================================
                    # ⚡ OPTIMIZATION: Combined filter (single pass)
                    # ==========================================
                    logging.info("\n🧹 Applying all validation filters...")
                    
                    # Build combined null filter using reduce
                    null_filter = reduce(
                        lambda a, b: a & b,
                        [F.col(col).isNotNull() for col in REQUIRED_COLUMNS]
                    )
                    
                    # Build quality filter (all conditions at once)
                    quality_filter = (
                        # Valid trip distance
                        (F.col("trip_distance") > 0) & 
                        (F.col("trip_distance") < 100) &
                        # Valid fare amount (if exists)
                        (F.col("fare_amount").isNull() | 
                         ((F.col("fare_amount") > 0) & (F.col("fare_amount") < 500))) &
                        # Valid passenger count
                        (F.col("passenger_count") > 0) &
                        (F.col("passenger_count") <= 6) &
                        # Dropoff after pickup
                        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
                        # Reasonable temperature (-20 to 45 Celsius)
                        (F.col("temperature_2m") >= -20) & 
                        (F.col("temperature_2m") <= 45) &
                        # Reasonable precipitation (0 to 100mm)
                        (F.col("precipitation") >= 0) & 
                        (F.col("precipitation") <= 100) &
                        # Reasonable wind speed (0 to 150 km/h)
                        (F.col("windspeed_10m") >= 0) & 
                        (F.col("windspeed_10m") <= 150) &
                        # Reasonable pressure (950 to 1050 hPa)
                        (F.col("pressure_msl") >= 950) & 
                        (F.col("pressure_msl") <= 1050)
                    )
                    
                    # ⚡ Apply BOTH filters in ONE operation
                    df_validated = df.filter(null_filter & quality_filter)
                    
                    # ==========================================
                    # ⚡ OPTIMIZATION: Cache for statistics calculation
                    # ==========================================
                    
                    # Now count only ONCE for validated data
                    final_count = df_validated.count()
                    removed = input_count - final_count
                    total_output += final_count
                    total_removed += removed
                    
                    logging.info(f"✅ Valid records: {final_count:,}")
                    logging.info(f"🗑️  Removed records: {removed:,} ({removed/input_count*100:.2f}%)")
                    
                    # ==========================================
                    # ⚡ OPTIMIZATION: Compute statistics in single pass
                    # ==========================================
                    logging.info("\n📊 Computing validation statistics...")
                    
                    # Store stats for reporting
                    month_stats = {
                        'year': year,
                        'month': month,
                        'input_count': input_count,
                        'output_count': final_count,
                        'removed_count': removed,
                        'removal_rate': removed/input_count*100 if input_count > 0 else 0
                    }
                    validation_stats.append(month_stats)    
                    # ==========================================
                    # Write validated data
                    # ==========================================
                    logging.info(f"\n💾 Writing validated data: {output_path}")
                    
                    df_validated.write \
                        .mode("append") \
                        .partitionBy("year", "month") \
                        .parquet(output_path)
                    
                    
                    logging.info(f"✅ Successfully validated {year}-{month:02d}: {final_count:,} records")
                    
                except Exception as e:
                    error_msg = str(e)
                    if "Path does not exist" in error_msg or "FileNotFoundException" in error_msg:
                        logging.info(f"ℹ️  No data for {year}-{month:02d} (file not found)")
                    else:
                        logging.warning(f"⚠️  Error processing {year}-{month:02d}: {e}")
                        import traceback
                        traceback.print_exc()
                    continue
        
        # ==========================================
        # Final Summary with Statistics
        # ==========================================
        print(f"\n{'='*100}")
        logging.info("🎉 DATA VALIDATION COMPLETED!")
        logging.info(f"📊 Total input records: {total_input:,}")
        logging.info(f"✅ Total validated records: {total_output:,}")
        logging.info(f"🗑️  Total removed records: {total_removed:,} ({total_removed/total_input*100:.2f}% removal rate)" if total_input > 0 else "🗑️  Total removed records: 0")
        logging.info(f"📂 Output location: {output_path}")
        
        # Display per-month statistics
        if validation_stats:
            logging.info("\n📊 Per-Month Validation Summary:")
            logging.info(f"{'Year-Month':<12} {'Input':>12} {'Output':>12} {'Removed':>12} {'Removal %':>12}")
            logging.info("-" * 65)
            for stat in validation_stats:
                logging.info(
                    f"{stat['year']}-{stat['month']:02d}      "
                    f"{stat['input_count']:>12,} "
                    f"{stat['output_count']:>12,} "
                    f"{stat['removed_count']:>12,} "
                    f"{stat['removal_rate']:>11.2f}%"
                )
        
        print("="*100)
        
        # ==========================================
        # ⚡ OPTIMIZATION: Final verification with single aggregation
        # ==========================================
        try:
            logging.info("\n🔍 Verifying final validated data...")
            df_result = spark.read.parquet(output_path)
            
            # Single aggregation for all final stats
            final_agg_exprs = [
                F.count("*").alias("total_records"),
                F.countDistinct("year", "month").alias("total_partitions"),
                F.avg("trip_distance").alias("avg_distance"),
                F.avg("temperature_2m").alias("avg_temp_c"),
                F.avg("precipitation").alias("avg_precip_mm"),
                F.avg("windspeed_10m").alias("avg_wind_kmh"),
                F.avg("pressure_msl").alias("avg_pressure_hpa"),
                # Final null check
                *[F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"null_{col}") 
                  for col in REQUIRED_COLUMNS]
            ]
            
            final_stats = df_result.agg(*final_agg_exprs).collect()[0]
            
            logging.info(f"\n✅ Final validated record count: {final_stats['total_records']:,}")
            logging.info(f"📁 Total partitions: {final_stats['total_partitions']}")
            
            logging.info("\n📊 Final Statistics:")
            logging.info(f"   Avg Distance: {final_stats['avg_distance']:.2f} km")
            logging.info(f"   Avg Temperature: {final_stats['avg_temp_c']:.1f}°C")
            logging.info(f"   Avg Precipitation: {final_stats['avg_precip_mm']:.2f} mm")
            logging.info(f"   Avg Wind Speed: {final_stats['avg_wind_kmh']:.1f} km/h")
            logging.info(f"   Avg Pressure: {final_stats['avg_pressure_hpa']:.1f} hPa")
            
            logging.info("\n🔍 Final null check (should be all zeros):")
            all_zero = True
            for col in REQUIRED_COLUMNS:
                null_count = final_stats[f"null_{col}"]
                if null_count > 0:
                    logging.error(f"   ❌ {col}: {null_count:,} nulls")
                    all_zero = False
            
            if all_zero:
                logging.info("   ✅ All required columns are 100% null-free!")
            
            logging.info("\n📁 Distribution by Year-Month:")
            df_result.groupBy("year", "month").count().orderBy("year", "month").show(24)
            
            logging.info("\n📋 Final Schema:")
            df_result.printSchema()
            
        except Exception as e:
            logging.error(f"⚠️  Could not verify output: {e}")
            import traceback
            traceback.print_exc()
        
        print("="*100)
        
    except Exception as e:
        logging.error(f"❌ Fatal error during validation: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        spark.stop()
        logging.info("🛑 Spark session stopped")

###############################################
# Main
###############################################
if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    
    ENDPOINT_URL_LOCAL = "minio:9000"
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']
    
    validate_data(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)