import sys
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, TimestampType, StringType
from pyspark.sql.window import Window
import pandas as pd
from scipy.spatial import cKDTree

# Setup Paths
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

###############################################
# Parameters & Arguments
###############################################
TAXI_LOOKUP_PATH = os.path.join(os.path.dirname(__file__), "data", "taxi_lookup.csv")
CFG_FILE = "config/datalake.yaml"

# Weather grid parameters
LAT_MIN, LAT_MAX = 40.5255, 40.8995
LON_MIN, LON_MAX = -74.2335, -73.7110
STEP = 0.1

# Process only 2018 for testing
YEARS = [2018]

###############################################

def get_spark_session(cfg, endpoint_url, access_key, secret_key):
    """
    Creates a Spark session configured for MinIO (S3A).
    """
    return SparkSession.builder \
        .appName("NYC_Taxi_Weather_Merge") \
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

def create_weather_grid():
    """
    Create weather grid points as numpy arrays for KD-Tree.
    Returns: grid_points (N, 2), point_ids (N,)
    """
    lats = np.arange(LAT_MIN, LAT_MAX + STEP, STEP)
    lons = np.arange(LON_MIN, LON_MAX + STEP, STEP)
    
    points = []
    point_ids = []
    pid = 0
    
    for lat in lats:
        for lon in lons:
            points.append([lat, lon])
            point_ids.append(pid)
            pid += 1
    
    grid_points = np.array(points)
    point_ids = np.array(point_ids)
    
    print(f"✅ Created weather grid with {len(point_ids):,} points")
    return grid_points, point_ids

def find_nearest_weather_point_udf(grid_points, point_ids):
    """
    Create a UDF that uses KD-Tree to find nearest weather point.
    Much faster than cross join + distance calculation.
    """
    # Build KD-Tree once (reused for all rows)
    tree = cKDTree(grid_points)
    
    def find_nearest(lat, lon):
        if lat is None or lon is None:
            return None
        # Query nearest neighbor (k=1)
        distance, index = tree.query([lat, lon], k=1)
        return int(point_ids[index])
    
    return F.udf(find_nearest, IntegerType())

def load_weather_data_for_month(spark, client, bucket_name, year, month):
    """
    Load weather data for a specific year-month.
    Validates that all required weather columns are present.
    """
    print(f"📥 Loading weather data for {year}-{month:02d}...")
    
    # Construct file path
    weather_file = f"weather/weather_{year}-{month:02d}.parquet"
    
    try:
        # Download to temporary location
        temp_path = f"/tmp/weather_{year}-{month:02d}.parquet"
        client.fget_object(bucket_name, weather_file, temp_path)
        
        # Read with pandas
        pdf = pd.read_parquet(temp_path)
        
        # Convert timestamp
        if 'time' in pdf.columns:
            pdf['time'] = pd.to_datetime(pdf['time'])
        
        # ==========================================
        # VERIFY WEATHER COLUMNS EXIST
        # ==========================================
        required_weather_cols = ['temperature_2m', 'precipitation', 'windspeed_10m', 'pressure_msl', 'point_id', 'time']
        missing = [col for col in required_weather_cols if col not in pdf.columns]
        
        if missing:
            print(f"❌ Missing weather columns: {missing}")
            print(f"📋 Available columns: {list(pdf.columns)}")
            raise ValueError(f"Weather data missing required columns: {missing}")
        else:
            print(f"✅ Weather columns present: {required_weather_cols}")
        
        # Clean up temp file
        os.remove(temp_path)
        
        # Convert to Spark DataFrame
        df_weather = spark.createDataFrame(pdf)
        
        # Verify schema
        print(f"📋 Weather schema:")
        df_weather.printSchema()
        
        print(f"✅ Loaded {len(pdf):,} weather records for {year}-{month:02d}")
        return df_weather
        
    except Exception as e:
        print(f"⚠️  Could not load weather data for {year}-{month:02d}: {e}")
        import traceback
        traceback.print_exc()
        return None

def merge_weather_data(endpoint_url, access_key, secret_key):
    """
    Merge weather data with taxi trip data month-by-month.
    Uses KD-Tree for efficient nearest neighbor search.
    Writes merged data to bucket_2/batch_with_weather (Silver layer with weather).
    """
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    # Initialize Spark
    spark = get_spark_session(cfg, endpoint_url, access_key, secret_key)
    
    # Initialize MinIO client
    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )
    minio_client = client.create_conn()

    try:
        # ==========================================
        # 1. Load Taxi Lookup (Location -> Lat/Lon)
        # ==========================================
        print(f"📖 Reading taxi zone lookup: {TAXI_LOOKUP_PATH}")
        
        if not os.path.exists(TAXI_LOOKUP_PATH):
            raise FileNotFoundError(f"taxi_lookup.csv not found at {TAXI_LOOKUP_PATH}")
        
        df_lookup = spark.read.csv(TAXI_LOOKUP_PATH, header=True, inferSchema=True) \
            .select("LocationID", "latitude", "longitude") \
            .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        
        print(f"✅ Loaded {df_lookup.count():,} valid location records")

        # ==========================================
        # 2. Create Weather Grid + KD-Tree
        # ==========================================
        grid_points, point_ids = create_weather_grid()
        nearest_point_udf = find_nearest_weather_point_udf(grid_points, point_ids)
        
        # ==========================================
        # 3. Process Each Year-Month
        # ==========================================
        # INPUT: bucket_2/batch (without weather)
        input_base = f"s3a://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}/"
        
        # OUTPUT: bucket_2/batch_with_weather (Silver layer with weather)
        output_path = f"s3a://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}_with_weather/"
        
        print(f"\n📥 Input: {input_base}")
        print(f"📤 Output: {output_path}")
        
        total_processed = 0
        
        for year in YEARS:
            for month in range(1, 13):
                print(f"\n{'='*100}")
                print(f"📅 Processing: {year}-{month:02d}")
                
                try:
                    # ==========================================
                    # 3a. Load Taxi Data for this month (from bucket_2 WITHOUT weather)
                    # ==========================================
                    taxi_path = f"{input_base}year={year}/month={month}/"
                    
                    print(f"📂 Reading taxi data: {taxi_path}")
                    df_taxi = spark.read.parquet(taxi_path)
                    
                    # DO NOT add partition columns here - they'll be added before writing
                    
                    taxi_count = df_taxi.count()
                    print(f"✅ Loaded {taxi_count:,} taxi records")
                    
                    # ==========================================
                    # 3b. Join Pickup Location
                    # ==========================================
                    print("🔗 Joining pickup location coordinates...")
                    df_taxi = df_taxi.join(F.broadcast(df_lookup), 
                                           df_taxi.pu_location_id == df_lookup.LocationID, 
                                           "left") \
                                     .withColumnRenamed("latitude", "pickup_latitude") \
                                     .withColumnRenamed("longitude", "pickup_longitude") \
                                     .drop("LocationID")
                    
                    # ==========================================
                    # 3c. Join Dropoff Location
                    # ==========================================
                    print("🔗 Joining dropoff location coordinates...")
                    df_taxi = df_taxi.join(F.broadcast(df_lookup), 
                                           df_taxi.do_location_id == df_lookup.LocationID, 
                                           "left") \
                                     .withColumnRenamed("latitude", "dropoff_latitude") \
                                     .withColumnRenamed("longitude", "dropoff_longitude") \
                                     .drop("LocationID")
                    
                    # ==========================================
                    # 3d. Calculate Midpoint
                    # ==========================================
                    print("🧮 Calculating trip midpoint...")
                    df_taxi = df_taxi.withColumn(
                        "midpoint_lat", 
                        (F.col("pickup_latitude") + F.col("dropoff_latitude")) / 2
                    ).withColumn(
                        "midpoint_lon", 
                        (F.col("pickup_longitude") + F.col("dropoff_longitude")) / 2
                    )
                    
                    # Round pickup time to hour
                    df_taxi = df_taxi.withColumn(
                        "pickup_hour",
                        F.date_trunc("hour", F.col("pickup_datetime"))
                    )
                    
                    # ==========================================
                    # 3e. Find Nearest Weather Point (KD-Tree UDF)
                    # ==========================================
                    print("🔍 Finding nearest weather points using KD-Tree...")
                    df_taxi = df_taxi.withColumn(
                        "weather_point_id",
                        nearest_point_udf(F.col("midpoint_lat"), F.col("midpoint_lon"))
                    )
                    
                    valid_weather_points = df_taxi.filter(F.col("weather_point_id").isNotNull()).count()
                    print(f"✅ Found weather points for {valid_weather_points:,} / {taxi_count:,} records ({valid_weather_points/taxi_count*100:.1f}%)")
                    
                    # ==========================================
                    # 3f. Load Weather Data for this month
                    # ==========================================
                    df_weather = load_weather_data_for_month(spark, minio_client, 
                                                             datalake_cfg['bucket_name_1'], 
                                                             year, month)
                    
                    if df_weather is None:
                        print(f"⚠️  Skipping {year}-{month:02d} - No weather data available")
                        continue
                    
                    # ==========================================
                    # 3g. Join Weather Data
                    # ==========================================
                    print("🔄 Joining weather data...")
                    print(f"📋 Taxi columns before join: {df_taxi.columns}")
                    print(f"📋 Weather columns: {df_weather.columns}")
                    
                    df_final = df_taxi.join(
                        df_weather,
                        (df_taxi.weather_point_id == df_weather.point_id) & 
                        (df_taxi.pickup_hour == df_weather.time),
                        "left"
                    )
                    
                    print(f"📋 Columns AFTER join: {df_final.columns}")
                    
                    # ==========================================
                    # VERIFY weather columns exist after join
                    # ==========================================
                    weather_cols_check = ['temperature_2m', 'precipitation', 'windspeed_10m', 'pressure_msl']
                    missing_after_join = [col for col in weather_cols_check if col not in df_final.columns]
                    
                    if missing_after_join:
                        print(f"❌ Weather columns MISSING after join: {missing_after_join}")
                        print("❌ JOIN FAILED - Weather data not merged!")
                        raise ValueError(f"Weather columns lost after join: {missing_after_join}")
                    else:
                        print(f"✅ Weather columns present after join: {weather_cols_check}")
                    
                    # Check how many records have weather data
                    weather_count = df_final.filter(F.col("temperature_2m").isNotNull()).count()
                    print(f"🌦️  Records with weather data: {weather_count:,} / {taxi_count:,} ({weather_count/taxi_count*100:.1f}%)")
                    
                    # ==========================================
                    # 3h. Clean up temporary columns (KEEP weather columns!)
                    # ==========================================
                    print("🧹 Cleaning up temporary columns...")
                    df_final = df_final.drop(
                        "pickup_latitude", "pickup_longitude",
                        "dropoff_latitude", "dropoff_longitude",
                        "midpoint_lat", "midpoint_lon",
                        "pickup_hour", "weather_point_id"
                    )
                    
                    # Drop duplicate columns from weather join if they exist
                    if "point_id" in df_final.columns:
                        df_final = df_final.drop("point_id")
                    if "time" in df_final.columns:
                        df_final = df_final.drop("time")
                    if "latitude" in df_final.columns:
                        df_final = df_final.drop("latitude")
                    if "longitude" in df_final.columns:
                        df_final = df_final.drop("longitude")
                    
                    print(f"📋 Final columns: {df_final.columns}")
                    
                    # Verify weather columns still present
                    final_weather_check = [col for col in weather_cols_check if col in df_final.columns]
                    print(f"✅ Weather columns retained: {final_weather_check}")
                    
                    # ==========================================
                    # 3i. Add partition columns BEFORE writing
                    # ==========================================
                    df_final = df_final.withColumn("year", F.lit(year)) \
                                       .withColumn("month", F.lit(month))
                    
                    # ==========================================
                    # 3j. Write this month's data to bucket_2/batch_with_weather
                    # ==========================================
                    print(f"💾 Writing merged data: {output_path}")
                    df_final.write \
                        .mode("append") \
                        .partitionBy("year", "month") \
                        .parquet(output_path)
                    
                    total_processed += taxi_count
                    print(f"✅ Successfully processed {year}-{month:02d}: {taxi_count:,} records with weather")
                    
                except Exception as e:
                    print(f"⚠️  Error processing {year}-{month:02d}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
        
        # ==========================================
        # 4. Final Summary & Verification
        # ==========================================
        print(f"\n{'='*100}")
        print(f"🎉 WEATHER MERGE COMPLETED!")
        print(f"📊 Total records processed: {total_processed:,}")
        print(f"📂 Output location: {output_path}")
        print("="*100)
        
        # Verify output
        try:
            print("\n🔍 Verifying merged data...")
            df_result = spark.read.parquet(output_path)
            
            print(f"\n📊 Final Schema:")
            df_result.printSchema()
            
            result_count = df_result.count()
            print(f"\n✅ Total records in output: {result_count:,}")
            
            print("\n📁 Distribution by Year-Month:")
            df_result.groupBy("year", "month").count().orderBy("year", "month").show(24)
            
            print("\n🌦️  Weather Data Coverage:")
            df_result.select(
                F.count("*").alias("total_records"),
                F.count("temperature_2m").alias("temp_count"),
                F.count("precipitation").alias("precip_count"),
                F.count("windspeed_10m").alias("wind_count"),
                F.count("pressure_msl").alias("pressure_count")
            ).show()
            
            # Calculate coverage percentage
            weather_coverage = df_result.filter(F.col("temperature_2m").isNotNull()).count()
            coverage_pct = (weather_coverage / result_count * 100) if result_count > 0 else 0
            print(f"📊 Weather coverage: {weather_coverage:,} / {result_count:,} ({coverage_pct:.1f}%)")
            
            print("\n📊 Sample Statistics:")
            df_result.select(
                F.avg("temperature_2m").alias("avg_temp_c"),
                F.avg("precipitation").alias("avg_precip_mm"),
                F.avg("windspeed_10m").alias("avg_wind_kmh"),
                F.avg("pressure_msl").alias("avg_pressure_hpa")
            ).show()
            
        except Exception as e:
            print(f"⚠️  Could not verify output: {e}")
            import traceback
            traceback.print_exc()
        
        print("="*100)
        
    except Exception as e:
        print(f"❌ Fatal error during weather merge: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        spark.stop()
        print("🛑 Spark session stopped")

###############################################
# Main
###############################################
if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    ENDPOINT_URL_LOCAL = "minio:9000"
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']

    merge_weather_data(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)