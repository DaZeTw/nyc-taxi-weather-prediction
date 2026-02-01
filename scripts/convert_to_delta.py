import sys
import os
import warnings
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "config/datalake.yaml"

###############################################

def get_spark_session(endpoint_url, access_key, secret_key):
    """
    Create Spark session with Delta Lake support and optimized settings
    """
    return SparkSession.builder \
        .appName("Delta_Lake_Conversion") \
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.0.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint_url}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
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

def delta_convert(endpoint_url, access_key, secret_key, years=[2024]):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    
    logging.info("🚀 Starting Delta Lake conversion...")
    
    spark = get_spark_session(endpoint_url, access_key, secret_key)
    logging.info('✅ Spark session created with Delta Lake support')

    client = MinIOClient(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)
    client.create_bucket(datalake_cfg['bucket_name_3'])
    logging.info(f"✅ Bucket '{datalake_cfg['bucket_name_3']}' ready")

    try:
        input_base = f"s3a://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}_validated/"
        output_path = f"s3a://{datalake_cfg['bucket_name_3']}/gold/"
        
        logging.info(f"📥 Input: {input_base}")
        logging.info(f"📤 Output: {output_path}")
        
        total_processed = 0
        
        for year in years:
            for month in range(1, 13):
                print(f"\n{'='*100}")
                logging.info(f"📅 Processing: {year}-{month:02d}")
                
                try:
                    input_path = f"{input_base}year={year}/month={month}/"
                    
                    logging.info(f"📂 Reading: {input_path}")
                    df = spark.read.parquet(input_path)
                    
                    # Add partition columns back
                    df = df.withColumn("year", F.lit(year)) \
                           .withColumn("month", F.lit(month))
                    
                    # ⚡ OPTIMIZATION: Count only once
                    record_count = df.count()
                    logging.info(f"✅ Loaded {record_count:,} records")
                    
                    # ==========================================
                    # REMOVED: Timestamp conversion - keep original type
                    # The timestamps are already in correct format from validated data
                    # ==========================================
                    logging.info("✅ Using original timestamp types (no conversion needed)")
                    
                    # ==========================================
                    # Verify weather columns exist
                    # ==========================================
                    weather_cols = ["temperature_2m", "precipitation", "windspeed_10m", "pressure_msl"]
                    missing_weather = [col for col in weather_cols if col not in df.columns]
                    
                    if missing_weather:
                        logging.error(f"❌ MISSING WEATHER COLUMNS: {missing_weather}")
                        logging.error("❌ Validated data does NOT contain weather! Pipeline broken!")
                        raise ValueError(f"Weather columns missing: {missing_weather}")
                    else:
                        logging.info(f"✅ Weather columns present: {weather_cols}")
                    
                    # ==========================================
                    # ⚡ OPTIMIZATION: Single-pass null check
                    # ==========================================
                    critical_columns = [
                        "taxi_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
                        "passenger_count", "trip_distance", "pu_location_id", "do_location_id"
                    ] + weather_cols
                    
                    # Build aggregation for null checks
                    null_agg_exprs = [
                        F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"null_{col}")
                        for col in critical_columns
                    ]
                    
                    null_stats = df.agg(*null_agg_exprs).collect()[0]
                    
                    null_found = False
                    for col in critical_columns:
                        null_count = null_stats[f"null_{col}"]
                        if null_count > 0:
                            logging.warning(f"⚠️  {col}: {null_count} nulls")
                            null_found = True
                    
                    if not null_found:
                        logging.info("✅ No nulls in critical columns")
                    
                    # ==========================================
                    # Feature Engineering
                    # ==========================================
                    logging.info("🔧 Creating features...")
                    df = df.withColumn(
                        "trip_duration_minutes",
                        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60
                    ).withColumn(
                        "hour_of_day",
                        F.hour("pickup_datetime")
                    ).withColumn(
                        "day_of_week",
                        F.dayofweek("pickup_datetime")
                    ).withColumn(
                        "is_weekend",
                        F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)
                    ).withColumn(
                        "avg_speed_mph",
                        F.when(F.col("trip_duration_minutes") > 0,
                               F.col("trip_distance") / (F.col("trip_duration_minutes") / 60))
                        .otherwise(None)
                    )
                    
                    # Filter invalid durations
                    df = df.filter(
                        (F.col("trip_duration_minutes") > 1) &
                        (F.col("trip_duration_minutes") < 180)
                    )
                    
                    # ⚡ OPTIMIZATION: Count once after filtering
                    final_count = df.count()
                    removed = record_count - final_count
                    if removed > 0:
                        logging.info(f"🗑️  Removed {removed:,} invalid records")
                    
                    logging.info(f"✅ Final count: {final_count:,}")
                    
                    # ==========================================
                    # Write to Delta with schema merge enabled
                    # ==========================================
                    logging.info(f"💾 Writing to Delta: {output_path}")
                    df.write \
                        .format("delta") \
                        .mode("append") \
                        .option("mergeSchema", "true") \
                        .option("overwriteSchema", "false") \
                        .partitionBy("year", "month") \
                        .save(output_path)
                    
                    total_processed += final_count
                    logging.info(f"✅ Success: {year}-{month:02d}")
                    
                except Exception as e:
                    error_msg = str(e)
                    if "Path does not exist" in error_msg or "FileNotFoundException" in error_msg:
                        logging.info(f"ℹ️  No data for {year}-{month:02d} (file not found)")
                    else:
                        logging.error(f"❌ Error {year}-{month:02d}: {e}")
                        import traceback
                        traceback.print_exc()
                    continue
        
        # ==========================================
        # Final Verification
        # ==========================================
        print(f"\n{'='*100}")
        logging.info("🎉 COMPLETED!")
        logging.info(f"📊 Total records: {total_processed:,}")
        logging.info(f"📂 Output: {output_path}")
        
        try:
            df_delta = spark.read.format("delta").load(output_path)
            
            logging.info("\n📊 Schema:")
            df_delta.printSchema()
            
            # ⚡ OPTIMIZATION: Single aggregation for all stats
            final_agg_exprs = [
                F.count("*").alias("total_records"),
                F.countDistinct("year", "month").alias("partitions"),
                F.avg("trip_distance").alias("avg_distance"),
                F.avg("trip_duration_minutes").alias("avg_duration"),
                F.avg("temperature_2m").alias("avg_temp"),
                F.avg("precipitation").alias("avg_precip"),
                F.avg("windspeed_10m").alias("avg_wind"),
                F.avg("pressure_msl").alias("avg_pressure")
            ]
            
            stats = df_delta.agg(*final_agg_exprs).collect()[0]
            
            logging.info(f"\n✅ Total records: {stats['total_records']:,}")
            logging.info(f"📁 Partitions: {stats['partitions']}")
            
            logging.info("\n📊 Statistics:")
            logging.info(f"   Avg Distance: {stats['avg_distance']:.2f} miles")
            logging.info(f"   Avg Duration: {stats['avg_duration']:.1f} minutes")
            logging.info(f"   Avg Temperature: {stats['avg_temp']:.1f}°C")
            logging.info(f"   Avg Precipitation: {stats['avg_precip']:.2f} mm")
            logging.info(f"   Avg Wind Speed: {stats['avg_wind']:.1f} km/h")
            logging.info(f"   Avg Pressure: {stats['avg_pressure']:.1f} hPa")
            
            logging.info("\n📁 Distribution by Year-Month:")
            df_delta.groupBy("year", "month").count().orderBy("year", "month").show(24)
            
        except Exception as e:
            logging.error(f"⚠️  Verification error: {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}")
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
    
    delta_convert(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)