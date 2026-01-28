import pandas as pd
import numpy as np
import requests
import os
import time

# 1. Configuration
OUTPUT_DIR = "data/weather"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Grid coordinates (from your cleaned lookup)
LAT_MIN, LAT_MAX = 40.5255, 40.8995
LON_MIN, LON_MAX = -74.2335, -73.7110
STEP = 0.1 

# Extended Time Range: 2018 to Sept 2025
START_DATE = "2018-01-01"
END_DATE   = "2025-09-30"

# 2. Generate Grid Points
lats = np.arange(LAT_MIN, LAT_MAX + STEP, STEP)
lons = np.arange(LON_MIN, LON_MAX + STEP, STEP)

points = []
pid = 0
for lat in lats:
    for lon in lons:
        points.append({"pid": pid, "lat": round(lat, 4), "lon": round(lon, 4)})
        pid += 1

print(f"🌍 Processing {len(points)} zones for the period {START_DATE} to {END_DATE}...")

# 3. Fetching Loop (Save individually and Skip existing)
for i, pt in enumerate(points):
    lat, lon, pid = pt["lat"], pt["lon"], pt["pid"]
    
    # Define file path for this specific point
    file_path = os.path.join(OUTPUT_DIR, f"point_{pid}.parquet")
    
    # Check if this point has already been successfully downloaded
    if os.path.exists(file_path):
        print(f"⏩ Skipping Point {pid} (Already exists at {file_path})")
        continue

    print(f"📡 Requesting zone {i+1}/{len(points)} (PID: {pid})...")

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={START_DATE}&end_date={END_DATE}"
        "&hourly=temperature_2m,precipitation,windspeed_10m,pressure_msl"
        "&timezone=UTC"
    )

    try:
        response = requests.get(url, timeout=60)
        
        # Handle Rate Limiting (429) specifically
        if response.status_code == 429:
            print(f"⚠️ Rate limit hit. Sleeping for 60 seconds before retrying...")
            time.sleep(60)
            # Try once more for this point after sleeping
            response = requests.get(url, timeout=60)

        response.raise_for_status()
        data = response.json()

        # Create DataFrame for this specific location
        df_zone = pd.DataFrame({
            "time": pd.to_datetime(data["hourly"]["time"]),
            "temperature_2m": data["hourly"]["temperature_2m"],
            "precipitation": data["hourly"]["precipitation"],
            "windspeed_10m": data["hourly"]["windspeed_10m"],
            "pressure_msl": data["hourly"]["pressure_msl"],
        })
        df_zone["point_id"] = pid
        df_zone["latitude"] = lat
        df_zone["longitude"] = lon
        
        # Save point data immediately to disk
        df_zone.to_parquet(file_path, index=False)
        print(f"✅ Saved point {pid} to {file_path}")
        
        # Respect API rate limits
        time.sleep(1.0) 

    except Exception as e:
        print(f"❌ Failed to fetch point {pid}: {e}")

print("🎉 Process finished. Check your data/weather folder.")


# import pandas as pd
# import glob
# import os

# # 1. Configuration
# DATA_DIR = "data/weather"
# # Find all point-based files downloaded previously
# point_files = glob.glob(os.path.join(DATA_DIR, "point_*.parquet"))

# def partition_by_month(files):
#     if not files:
#         print("❌ No point files found in data/weather. Please run the fetcher first.")
#         return

#     print(f"📂 Found {len(files)} point files. Starting consolidation...")

#     # 2. Load and Combine
#     # We load all points to ensure we have the full geographic grid for each month
#     all_points_df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    
#     # 3. Extract Year-Month for partitioning
#     # The 'time' column was saved as a standardized timestamp
#     all_points_df['year_month'] = all_points_df['time'].dt.strftime('%Y-%m')

#     # 4. Group and Save
#     unique_months = all_points_df['year_month'].unique()
#     print(f"📅 Detected {len(unique_months)} unique months. Saving to monthly files...")

#     for month in unique_months:
#         month_df = all_points_df[all_points_df['year_month'] == month].copy()
        
#         # Define the Silver-layer filename
#         output_filename = f"weather_{month}.parquet"
#         output_path = os.path.join(DATA_DIR, output_filename)
        
#         # Remove the helper column before saving
#         month_df.drop(columns=['year_month'], inplace=True)
        
#         # Save as Parquet for storage efficiency
#         month_df.to_parquet(output_path, index=False)
#         print(f"💾 Saved: {output_filename}")

# if __name__ == "__main__":
#     partition_by_month(point_files)
#     print("✅ Partitioning complete. Your monthly weather files are ready.")

# import pandas as pd
# import glob
# import os

# # 1. Configuration
# DATA_DIR = "data/weather"
# point_files = glob.glob(os.path.join(DATA_DIR, "point_*.parquet"))

# def inspect_weather_data(files):
#     if not files:
#         print("❌ No point files found. Please run the fetcher script first.")
#         return

#     total_records = 0
#     sample_dfs = []

#     print(f"🔍 Inspecting {len(files)} point files...")

#     for f in files:
#         df = pd.read_parquet(f)
#         total_records += len(df)
#         # Collect a small sample from each point for inspection
#         sample_dfs.append(df.head(2))

#     # Combine samples for a quick look
#     inspection_df = pd.concat(sample_dfs, ignore_index=True)

#     print("\n--- Data Summary ---")
#     print(f"📊 Total Records Across All Points: {total_records:,}")
#     print(f"📍 Unique Geographic Points: {len(files)}")
#     print(f"📅 Time Range per Point: {len(df):,} hours (~7.7 years)")
    
#     print("\n--- Sample Data Points ---")
#     print(inspection_df.head(10))
    
#     # Check for missing values
#     print("\n--- Missing Value Check ---")
#     print(inspection_df.isnull().sum())

# if __name__ == "__main__":
#     inspect_weather_data(point_files)