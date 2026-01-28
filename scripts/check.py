import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg

###############################################
# Configuration
###############################################
CFG_FILE = "config/datalake.yaml"

def get_spark_session(endpoint_url, access_key, secret_key):
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Check_Data") \
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
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def check_bucket_2_no_weather(spark, bucket_name, folder_name):
    """Check data in bucket_2/batch (WITHOUT weather)"""
    print("\n" + "="*100)
    print("📦 BUCKET 2 - PROCESSED PARQUET (NO WEATHER)")
    print("="*100)
    
    path = f"s3a://{bucket_name}/{folder_name}/"
    print(f"📂 Path: {path}\n")
    
    try:
        # Read all data
        df = spark.read.parquet(path)
        
        # Basic info
        print(f"✅ Total records: {df.count():,}")
        print(f"📊 Total columns: {len(df.columns)}")
        
        # Schema
        print("\n📋 Schema:")
        df.printSchema()
        
        # Distribution by year-month
        print("\n📅 Distribution by Year-Month:")
        df.groupBy("year", "month").count().orderBy("year", "month").show(24)
        
        # Sample data
        print("\n🔍 Sample Data (5 records):")
        df.show(5, truncate=False)
        
        # Statistics
        print("\n📊 Basic Statistics:")
        df.select(
            F.count("*").alias("total_records"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("passenger_count").alias("avg_passengers"),
            F.min("pickup_datetime").alias("earliest_trip"),
            F.max("pickup_datetime").alias("latest_trip")
        ).show(truncate=False)
        
        # Value counts for categorical columns
        print("\n🚕 Taxi Type Distribution:")
        df.groupBy("taxi_type").count().orderBy("count", ascending=False).show()
        
        print("\n👥 Passenger Count Distribution:")
        df.groupBy("passenger_count").count().orderBy("passenger_count").show()
        
        print("\n💳 Payment Type Distribution:")
        df.groupBy("payment_type").count().orderBy("count", ascending=False).show()
        
        # Check for nulls
        print("\n🔍 Null Value Check:")
        null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
        null_counts.show(vertical=True)
        
    except Exception as e:
        print(f"❌ Error reading bucket_2 (no weather): {e}")
        import traceback
        traceback.print_exc()

def check_bucket_2_with_weather(spark, bucket_name, folder_name):
    """Check data in bucket_2/batch_with_weather (WITH weather)"""
    print("\n" + "="*100)
    print("📦 BUCKET 2 - PARQUET WITH WEATHER (SILVER LAYER)")
    print("="*100)
    
    path = f"s3a://{bucket_name}/{folder_name}_with_weather/"
    print(f"📂 Path: {path}\n")
    
    try:
        # Read all data
        df = spark.read.parquet(path)
        
        # Basic info
        print(f"✅ Total records: {df.count():,}")
        print(f"📊 Total columns: {len(df.columns)}")
        
        # Schema
        print("\n📋 Schema:")
        df.printSchema()
        
        # Distribution by year-month
        print("\n📅 Distribution by Year-Month:")
        df.groupBy("year", "month").count().orderBy("year", "month").show(24)
        
        # Sample data
        print("\n🔍 Sample Data (5 records):")
        df.show(5, truncate=False)
        
        # Statistics with weather
        print("\n📊 Statistics (Taxi + Weather):")
        df.select(
            F.count("*").alias("total_records"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("temperature_2m").alias("avg_temp_c"),
            F.avg("precipitation").alias("avg_precip_mm"),
            F.avg("windspeed_10m").alias("avg_wind_kmh")
        ).show(truncate=False)
        
        # Weather data quality
        print("\n🌦️  Weather Data Quality:")
        df.select(
            F.count("temperature_2m").alias("temp_count"),
            F.count("precipitation").alias("precip_count"),
            F.count("windspeed_10m").alias("wind_count"),
            F.count("*").alias("total")
        ).show()
        
        # Weather stats by month
        print("\n🌡️  Average Temperature by Month:")
        df.groupBy("month") \
            .agg(F.avg("temperature_2m").alias("avg_temp")) \
            .orderBy("month").show(12)
        
        # Check for nulls
        print("\n🔍 Null Value Check:")
        null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
        null_counts.show(vertical=True)
        
    except Exception as e:
        print(f"❌ Error reading bucket_2 (with weather): {e}")
        import traceback
        traceback.print_exc()

def check_bucket_3_delta(spark, bucket_name):
    """Check Delta Lake data in bucket_3/gold (Gold Layer)"""
    print("\n" + "="*100)
    print("📦 BUCKET 3 - DELTA LAKE (GOLD LAYER)")
    print("="*100)
    
    path = f"s3a://{bucket_name}/gold/"
    print(f"📂 Path: {path}\n")
    
    try:
        # Read Delta table
        df = spark.read.format("delta").load(path)
        
        # Basic info
        print(f"✅ Total records: {df.count():,}")
        print(f"📊 Total columns: {len(df.columns)}")
        
        # Schema
        print("\n📋 Schema:")
        df.printSchema()
        
        # Distribution by year-month
        print("\n📅 Distribution by Year-Month:")
        df.groupBy("year", "month").count().orderBy("year", "month").show(24)
        
        # Sample data
        print("\n🔍 Sample Data (5 records):")
        df.show(5, truncate=False)
        
        # Statistics with engineered features
        print("\n📊 Statistics (with Engineered Features):")
        df.select(
            F.count("*").alias("total_records"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
            F.avg("avg_speed_mph").alias("avg_speed"),
            F.avg("temperature_2m").alias("avg_temp"),
            F.sum("is_weekend").alias("weekend_trips")
        ).show(truncate=False)
        
        # Hour distribution
        print("\n🕐 Hour of Day Distribution:")
        df.groupBy("hour_of_day").count().orderBy("hour_of_day").show(24)
        
        # Day of week distribution
        print("\n📆 Day of Week Distribution (1=Sunday, 7=Saturday):")
        df.groupBy("day_of_week") \
            .agg(F.count("*").alias("trip_count"),
                 F.avg("fare_amount").alias("avg_fare")) \
            .orderBy("day_of_week").show()
        
        # Weekend vs Weekday
        print("\n🎯 Weekend vs Weekday:")
        df.groupBy("is_weekend") \
            .agg(F.count("*").alias("trip_count"),
                 F.avg("fare_amount").alias("avg_fare"),
                 F.avg("trip_duration_minutes").alias("avg_duration")) \
            .show()
        
        # Speed analysis
        print("\n🚗 Speed Analysis:")
        df.select(
            F.min("avg_speed_mph").alias("min_speed"),
            F.avg("avg_speed_mph").alias("avg_speed"),
            F.max("avg_speed_mph").alias("max_speed")
        ).show()
        
        # Check for nulls
        print("\n🔍 Null Value Check:")
        null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
        null_counts.show(vertical=True)
        
    except Exception as e:
        print(f"❌ Error reading bucket_3 delta: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to check all data layers"""
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    
    ENDPOINT_URL = "minio:9000"
    ACCESS_KEY = datalake_cfg['access_key']
    SECRET_KEY = datalake_cfg['secret_key']
    
    print("\n" + "="*100)
    print("🔍 NYC TAXI DATA QUALITY CHECK")
    print("="*100)
    
    # Create Spark session
    spark = get_spark_session(ENDPOINT_URL, ACCESS_KEY, SECRET_KEY)
    
    try:
        # 1. Check bucket_2 WITHOUT weather (Bronze layer)
        check_bucket_2_no_weather(spark, datalake_cfg['bucket_name_2'], datalake_cfg['folder_name'])
        
        # 2. Check bucket_2 WITH weather (Silver layer)
        check_bucket_2_with_weather(spark, datalake_cfg['bucket_name_2'], datalake_cfg['folder_name'])
        
        # 3. Check bucket_3 Delta Lake (Gold layer)
        check_bucket_3_delta(spark, datalake_cfg['bucket_name_3'])
        
        print("\n" + "="*100)
        print("✅ DATA CHECK COMPLETED!")
        print("="*100)
        print("\n📊 Summary:")
        print("   1. bucket_2/batch/              → Processed data (NO weather)")
        print("   2. bucket_2/batch_with_weather/ → Silver layer (WITH weather)")
        print("   3. bucket_3/gold/               → Gold layer (Delta + Features)")
        print("="*100 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
        print("🛑 Spark session stopped")

if __name__ == "__main__":
    main()