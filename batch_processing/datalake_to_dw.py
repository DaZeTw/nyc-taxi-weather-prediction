import logging
import os
import sys
import time
import traceback
import warnings

import dotenv
dotenv.load_dotenv(".env")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "utils"))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
warnings.filterwarnings("ignore")

###############################################
# Parameters & Arguments
###############################################
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Target staging table
DB_STAGING_TABLE = "staging.nyc_taxi_weather"

# MinIO Configuration
CFG_FILE = "./config/datalake.yaml"
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]

# Source bucket (Delta Lake - Gold layer)
BUCKET_NAME = datalake_cfg["bucket_name_3"]  # sandbox bucket with gold/
DELTA_PATH = "gold/"

# Spark Configuration
CFG_FILE_SPARK = "./config/spark.yaml"
cfg = load_cfg(CFG_FILE_SPARK)
spark_cfg = cfg.get("spark_config", {})

MEMORY = spark_cfg.get("executor_memory", "4g")

# Process configuration
YEARS = [2018]  # Years to process
###############################################


###############################################
# PySpark Session
###############################################
def create_spark_session():
    """
    Create Spark Session with Delta Lake and PostgreSQL support
    """
    try:
        spark = SparkSession.builder \
            .appName("Delta_to_Staging_Pipeline") \
            .config("spark.executor.memory", MEMORY) \
            .config("spark.driver.memory", "4g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.sql.shuffle.partitions", "16") \
            .config("spark.default.parallelism", "16") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.0.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                    "org.postgresql:postgresql:42.4.3") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()

        logging.info("✅ Spark session successfully created!")
        return spark

    except Exception as e:
        logging.error(f"❌ Failed to create Spark session: {e}")
        traceback.print_exc()
        raise


###############################################
# Data Processing
###############################################
def prepare_staging_data(df):
    """
    Prepare data for staging.nyc_taxi_weather table
    Select and cast columns to match PostgreSQL schema
    """
    try:
        # Select columns that exist in Delta Lake
        df_staging = df.select(
            # Partition columns
            "year",
            "month",
            
            # Taxi identifiers
            "taxi_type",
            "vendor_id",
            
            # Trip timestamps
            "pickup_datetime",
            "dropoff_datetime",
            
            # Trip details
            "passenger_count",
            F.col("trip_distance").cast("float"),
            "rate_code_id",
            "store_and_fwd_flag",
            
            # Location
            "pu_location_id",
            "do_location_id",
            
            # Payment
            "payment_type",
            F.col("fare_amount").cast("float"),
            F.col("extra").cast("float"),
            F.col("mta_tax").cast("float"),
            F.col("tip_amount").cast("float"),
            F.col("tolls_amount").cast("float"),
            F.col("total_amount").cast("float"),
            F.col("improvement_surcharge").cast("float"),
            # NOTE: congestion_surcharge is NOT in Delta Lake - skipped
            
            # Weather data
            F.col("temperature_2m").cast("float"),
            F.col("precipitation").cast("float"),
            F.col("windspeed_10m").cast("float"),
            F.col("pressure_msl").cast("float"),
            
            # Derived features
            F.col("trip_duration_minutes").cast("float"),
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            F.col("avg_speed_mph").cast("float")
        )
        
        logging.info(f"✅ Prepared staging data: {df_staging.count():,} records")
        
        return df_staging
        
    except Exception as e:
        logging.error(f"❌ Error preparing staging data: {e}")
        traceback.print_exc()
        raise


def load_to_staging_table(df):
    """
    Load DataFrame to PostgreSQL staging table
    """
    try:
        URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        
        properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
            "batchsize": "10000",
            "reWriteBatchedInserts": "true"  # Optimize batch inserts
        }
        
        logging.info(f"💾 Writing {df.count():,} records to {DB_STAGING_TABLE}...")
        
        df.write.jdbc(
            url=URL,
            table=DB_STAGING_TABLE,
            mode="append",
            properties=properties
        )
        
        logging.info(f"✅ Successfully loaded data to {DB_STAGING_TABLE}")
        
    except Exception as e:
        logging.error(f"❌ Failed to load data to {DB_STAGING_TABLE}: {e}")
        traceback.print_exc()
        raise


###############################################
# Main Pipeline
###############################################
def main():
    """
    Main ETL pipeline: Delta Lake (MinIO) → PostgreSQL Staging
    Process data incrementally by year and month
    """
    start_time = time.time()
    
    logging.info("🚀 Starting Delta Lake to Staging pipeline (incremental by month)...")
    
    # Initialize Spark
    spark = create_spark_session()
    
    total_records_loaded = 0
    
    try:
        delta_path = f"s3a://{BUCKET_NAME}/{DELTA_PATH}"
        logging.info(f"📥 Source: {delta_path}")
        
        # ==========================================
        # Process each year and month
        # ==========================================
        for year in YEARS:
            for month in range(1, 13):
                print(f"\n{'='*100}")
                logging.info(f"📅 Processing: {year}-{month:02d}")
                
                try:
                    # Read Delta Lake with partition filter
                    df_delta = spark.read.format("delta").load(delta_path) \
                        .filter((F.col("year") == year) & (F.col("month") == month))
                    
                    # Check if data exists
                    month_count = df_delta.count()
                    
                    if month_count == 0:
                        logging.info(f"   ℹ️  No data for {year}-{month:02d}, skipping...")
                        continue
                    
                    logging.info(f"   ✅ Loaded {month_count:,} records for {year}-{month:02d}")
                    
                    # Show schema on first iteration
                    if year == YEARS[0] and month == 1:
                        logging.info("\n📊 Delta Lake Schema:")
                        df_delta.printSchema()
                    
                    # ==========================================
                    # Data Quality Check
                    # ==========================================
                    logging.info("   🔍 Performing data quality checks...")
                    
                    critical_columns = [
                        "taxi_type", "vendor_id", "pickup_datetime", "dropoff_datetime",
                        "trip_distance", "pu_location_id", "do_location_id",
                        "temperature_2m", "precipitation", "windspeed_10m", "pressure_msl"
                    ]
                    
                    null_check_exprs = [
                        F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"null_{col}")
                        for col in critical_columns
                    ]
                    
                    null_stats = df_delta.agg(*null_check_exprs).collect()[0]
                    
                    has_nulls = False
                    for col in critical_columns:
                        null_count = null_stats[f"null_{col}"]
                        if null_count > 0:
                            logging.warning(f"      ⚠️  {col}: {null_count:,} nulls ({null_count/month_count*100:.2f}%)")
                            has_nulls = True
                    
                    if not has_nulls:
                        logging.info("      ✅ All critical columns are null-free")
                    
                    # ==========================================
                    # Prepare and Load to Staging
                    # ==========================================
                    logging.info("   📊 Preparing data for staging...")
                    df_staging = prepare_staging_data(df_delta)
                    
                    load_to_staging_table(df_staging)
                    
                    total_records_loaded += month_count
                    logging.info(f"   ✅ Success: {year}-{month:02d}")
                    
                except Exception as e:
                    logging.error(f"   ❌ Error processing {year}-{month:02d}: {e}")
                    traceback.print_exc()
                    continue
        
        # ==========================================
        # Final Summary
        # ==========================================
        print(f"\n{'='*100}")
        logging.info("🎉 Pipeline completed successfully!")
        logging.info(f"📊 Total records loaded: {total_records_loaded:,}")
        
        elapsed_time = time.time() - start_time
        logging.info(f"⏱️  Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        
        # Verify data in staging
        logging.info("\n📊 Verifying staging table...")
        df_verify = spark.read.jdbc(
            url=f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
            table=DB_STAGING_TABLE,
            properties={
                "user": POSTGRES_USER,
                "password": POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver"
            }
        )
        
        verify_count = df_verify.count()
        logging.info(f"✅ Staging table contains: {verify_count:,} total records")
        
        # Show distribution by year-month
        logging.info("\n📁 Distribution by Year-Month:")
        df_verify.groupBy("year", "month").count() \
            .orderBy("year", "month") \
            .show(24, truncate=False)
        
    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}")
        traceback.print_exc()
        raise
        
    finally:
        spark.stop()
        logging.info("🛑 Spark session stopped")


###############################################
# Entry Point
###############################################
if __name__ == "__main__":
    main()