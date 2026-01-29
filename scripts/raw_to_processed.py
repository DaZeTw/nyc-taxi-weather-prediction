import sys
import os
import logging
import pandas as pd
import s3fs
from glob import glob
from pathlib import Path

import math
import numpy as np
from sklearn.neighbors import BallTree

BASE_DIR = Path(__file__).resolve().parent 
UTILS_PATH = BASE_DIR.parent / "utils"              
sys.path.append(str(UTILS_PATH))

from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

###############################################
# Parameters & Arguments
##############################################
CFG_FILE = "./config/datalake.yaml"
#
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

TAXI_DATA_PATH = BASE_DIR.parent / "data" / "nyc_taxi"
WEATHER_DATA_PATH = BASE_DIR.parent / "data" / "weather"
YEARS = ["2020", "2021", "2022", "2023", "2024"]
NEW_YEAR = "2025"
# YEARS = ["2023", "2024"]
TAXI_LOOKUP_PATH = TAXI_DATA_PATH / "taxi_lookup.csv"
WEATHER_LOOKUP_PATH = WEATHER_DATA_PATH / "weather_lookup.csv"

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']
###############################################


###############################################
# Process data
###############################################
def drop_column(df, file):
    """
        Drop columns 'store_and_fwd_flag'
    """
    drop_columns = ['store_and_fwd_flag', 'VendorID', 'PULocationID', 'DOLocationID', \
                    'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude', 'dropoff_time', \
                    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',  \
                    'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type']
    df = df.drop(columns=drop_columns, errors='ignore')
    print("Dropped columns: " + ", ".join(drop_columns) + " from file: " + file)
    return df

def drop_nulls(df):
    """
    Drop rows where midpoint_latitude or midpoint_longitude is null/NaN.
    """
    before = len(df)
    # # Remove missing data
    df = df.dropna()
    df = df.reindex(sorted(df.columns), axis=1)
    after = len(df)
    print(f"Dropped {before-after} rows with null values.")
    return df

def drop_invalid_time_rows(df, file):
    """
    Drop rows where:
        - dropoff_time is earlier than pickup_time.
        - pickup_time or dropoff_time is not 
    """
    file_name = file.split('/')[-1]
    parts = file_name.split('_')  # Split by '_'
    date_part = parts[-1].split('.')[0]  # Get '2024-12' from '2024-12.parquet'
    try:
        year, month = map(int, date_part.split('-'))
    except ValueError:
        print(f"Invalid date format in filename: {file_name}. Skipping year-month check.")
        year, month = None, None

    before = len(df)

    # Filter conditions
    valid_rows = (
        (df['dropoff_time'] >= df['pickup_time']) &  # Existing check
        (df['pickup_time'].notna()) & (df['dropoff_time'].notna())  # No NaT
    )

    # Add year-month check if extracted
    if year is not None and month is not None:
        valid_rows &= (
            (df['pickup_time'].dt.year == year) &
            (df['pickup_time'].dt.month == month) &
            (df['dropoff_time'].dt.year == year) &
            (df['dropoff_time'].dt.month == month)
        )
    
    df = df[valid_rows]
    after = len(df)
    print(f"Dropped {before-after} rows with invalid time data.")
    return df

def drop_invalid_coordinates(df):
    """
    Lọc bỏ các dòng có tọa độ pickup hoặc dropoff không hợp lệ.
    Giả sử tọa độ hợp lệ là:
      Latitude: 40.48 đến 40.93
      Longitude: -74.28 đến -73.65
    """
    before = len(df)
    # Khoảng tọa độ bao trọn NYC + 3 Sân bay lớn (JFK, LGA, EWR) + Central Park
    lat_min, lat_max = 40.48, 40.93
    lon_min, lon_max = -74.28, -73.65
    valid_rows = (
        (df["pickup_latitude"] >= lat_min) & (df["pickup_latitude"] <= lat_max) &
        (df["pickup_longitude"] >= lon_min) & (df["pickup_longitude"] <= lon_max) &
        (df["dropoff_latitude"] >= lat_min) & (df["dropoff_latitude"] <= lat_max) &
        (df["dropoff_longitude"] >= lon_min) & (df["dropoff_longitude"] <= lon_max)
    )
    df = df[valid_rows]
    after = len(df)
    print(f"Dropped {before-after} rows with invalid coordinate data.")
    return df

def drop_lower_outliers(df, columns=['trip_duration', 'trip_distance_miles'], lower_quantile=0.05):
    """
    Drop các dòng có giá trị ở cột thấp hơn ngưỡng lower_quantile.
    """
    for column in columns:
        threshold = df[column].quantile(lower_quantile)
        before = len(df)
    df = df[df[column] >= threshold]
    after = len(df)
    print(f"Dropped {before - after} rows below {lower_quantile*100}% quantile in column '{column}'.")
    return df

def add_lat_lon_from_lookup(df):
    """
    Thêm pickup_latitude/pickup_longitude và dropoff_latitude/dropoff_longitude
    dựa trên PU/D O location ID trong df và file taxi_lookup.csv
    """
    lookup = pd.read_csv(TAXI_LOOKUP_PATH)
    lookup = lookup[['LocationID', 'latitude', 'longitude']].drop_duplicates()
    lookup['LocationID'] = pd.to_numeric(lookup['LocationID'], errors='coerce')
    lat_map = lookup.set_index('LocationID')['latitude'].to_dict()
    lon_map = lookup.set_index('LocationID')['longitude'].to_dict()
    df["PULocationID"] = df["PULocationID"].astype(int)
    df["DOLocationID"] = df["DOLocationID"].astype(int)

    df['pickup_latitude']  = df['PULocationID'].map(lambda x: lat_map.get(int(x)) if pd.notna(x) else None)
    df['pickup_longitude'] = df['PULocationID'].map(lambda x: lon_map.get(int(x)) if pd.notna(x) else None)
    df['dropoff_latitude']  = df['DOLocationID'].map(lambda x: lat_map.get(int(x)) if pd.notna(x) else None)
    df['dropoff_longitude'] = df['DOLocationID'].map(lambda x: lon_map.get(int(x)) if pd.notna(x) else None)

    return df

def add_midpoint(df):
    """
    Tính midpoint_latitude và midpoint_longitude từ pickup/dropoff lat/lon
    """
    # đảm bảo là số
    for col in ['pickup_latitude','dropoff_latitude','pickup_longitude','dropoff_longitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    # tính trung điểm (NaN sẽ được xử lý tự động)
    df['midpoint_latitude']  = df[['pickup_latitude','dropoff_latitude']].mean(axis=1)
    df['midpoint_longitude'] = df[['pickup_longitude','dropoff_longitude']].mean(axis=1)
    return df

def add_trip_duration(df, unit='minutes', inplace=True):
    """
    Tính thời lượng chuyến từ pickup/dropoff datetime.
    unit: 'seconds' hoặc 'minutes'
    Trả về df với cột 'trip_duration_seconds' hoặc 'trip_duration_minutes'.
    """
    import pandas as pd
    df['pickup_time'] = pd.to_datetime(df['pickup_time'], errors='coerce')
    df['dropoff_time'] = pd.to_datetime(df['dropoff_time'], errors='coerce')

    dur_seconds = (df['dropoff_time'] - df['pickup_time']).dt.total_seconds()

    if unit == 'seconds':
        df['trip_duration'] = dur_seconds
    else:
        df['trip_duration'] = dur_seconds / 60.0

    return df if inplace else df.copy()

# Functions to merge weather data
def haversine(lat1, lon1, lat2, lon2):
    """
    Tính khoảng cách Haversine giữa hai điểm (lat, lon) theo km.
    """
    R = 6371  # Bán kính Trái Đất (km)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def find_nearest_point_id(df, lookup_path=WEATHER_LOOKUP_PATH):
    """
    Thêm cột 'nearest_point_id' vào df dựa trên khoảng cách Haversine gần nhất từ midpoint tới các điểm trong lookup.
    Sử dụng BallTree để tối ưu hiệu suất với dữ liệu lớn.
    """
    lookup = pd.read_csv(lookup_path)
    midpoints = df[['midpoint_latitude', 'midpoint_longitude']].values
    points = lookup[['latitude', 'longitude']].values
    
    # Chuyển sang radians cho BallTree
    midpoints_rad = np.radians(midpoints)
    points_rad = np.radians(points)
    
    # Xây dựng BallTree với metric Haversine
    tree = BallTree(points_rad, metric='haversine')
    
    # Tìm nearest neighbor
    distances, indices = tree.query(midpoints_rad, k=1)
    
    # Thêm cột nearest_point_id
    df['nearest_point_id'] = lookup.iloc[indices.flatten()]['point_id'].values
    return df

def add_pickup_hour_rounded(df):
    """
    Thêm cột 'pickup_hour_rounded' bằng cách làm tròn pickup_time tới giờ gần nhất và lấy giờ.
    """
    df['pickup_hour_rounded'] = df['pickup_time'].dt.round('h')
    return df

def get_matching_weather_file(taxi_file, weather_data_path):
    file_name = taxi_file.split('/')[-1]
    parts = file_name.split('_')  # Split by '_'
    date_part = parts[-1].split('.')[0]  # Get '2024-12' from '2024-12.parquet'
    year, month = map(int, date_part.split('-'))
    weather_file_name = weather_data_path / f"{year}" / f"weather_{date_part}.parquet"
    return weather_file_name

def add_weather_data(df, weather_path):
    """
    Add weather data to the taxi DataFrame based on nearest_point_id and pickup_hour_rounded.
    """
    # Load weather data
    weather_df = pd.read_parquet(weather_path)
    
    # Merge on nearest_point_id and pickup_hour_rounded
    merged_df = df.merge(weather_df, left_on=['nearest_point_id', 'pickup_hour_rounded'], right_on=['point_id', 'time'], how='left')
    
    # Drop the extra columns from weather_df if necessary
    merged_df.drop(columns=['nearest_point_id', 'pickup_hour_rounded', 'point_id', 'time', 'latitude', 'longitude'], inplace=True, errors='ignore')
    
    return merged_df

def process(df, file):
    """
    Green:
        Rename column: lpep_pickup_datetime, lpep_dropoff_datetime, ehail_fee
        Drop: trip_type
    Yellow:
        Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
    """
    
    if file.startswith("green"):
        # rename columns
        df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_time",
                "lpep_dropoff_datetime": "dropoff_time",
                "ehail_fee": "fee",
                'RatecodeID': 'rate_code_id',
                'trip_distance': 'trip_distance_miles'
            },
            inplace=True
        )

        # add column
        df["taxi_type"] = "green"

        # drop column
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)

    elif file.startswith("yellow"):
        # rename columns
        if "Airport_fee" in df.columns:
            df.rename(
                columns={
                    "tpep_pickup_datetime": "pickup_time",
                    "tpep_dropoff_datetime": "dropoff_time",
                    "Airport_fee": "fee",
                    'RatecodeID': 'rate_code_id',
                    'trip_distance': 'trip_distance_miles'
                },
                inplace=True
            )
        else:
            df.rename(
                columns={
                    "tpep_pickup_datetime": "pickup_time",
                    "tpep_dropoff_datetime": "dropoff_time",
                    "airport_fee": "fee",
                    'RatecodeID': 'rate_code_id',
                    'trip_distance': 'trip_distance_miles'
                },
                inplace=True
            )

        # add column
        df["taxi_type"] = "yellow"

    # drop column 'fee'
    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)
    
    print("Transformed data from file: " + file)

    return df

###############################################


###############################################
# Process data
###############################################
def main():
    # Create bucket 'processed'
    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )
    client.create_bucket(BUCKET_NAME_2)
    logger.info("Ensured bucket exists: %s", BUCKET_NAME_2)

    # s3fs configured to talk to MinIO
    s3_fs = s3fs.S3FileSystem(
        anon=False,
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={'endpoint_url': "http://" + MINIO_ENDPOINT}
    )

    for year in YEARS:
        pattern = str(TAXI_DATA_PATH / year / "*.parquet")
        files = glob(pattern)
        if not files:
            logger.warning("No parquet files found for year %s at %s", year, pattern)
            continue

        for file in files:
            file_name = file.split('\\')[-1]
            print(f"Reading parquet file: {file_name}")

            # path = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['taxi_folder_name']}/" + file_name
            # if path in s3_fs.ls(f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['taxi_folder_name']}/"):
            #     print(f"File already processed and exists at {path}, skipping...")
            #     continue

            df = pd.read_parquet(file, engine='pyarrow')

            # lower case all columns
            # df.columns = map(str.lower, df.columns)

            # df = process(df, file_name)
            # df = merge_taxi_zone(df, file_name)
            # df = add_midpoint(df)
            # df = drop_column(df, file_name)

            df = process(df, file_name)
            df = add_lat_lon_from_lookup(df)
            df = add_midpoint(df)
            df = add_trip_duration(df, unit='seconds', inplace=True)
            df = drop_nulls(df)
            df = drop_invalid_time_rows(df, file_name)

            # Merge weather data
            df = find_nearest_point_id(df)
            df = add_pickup_hour_rounded(df)
            df = drop_invalid_coordinates(df)
            weather_file_path = get_matching_weather_file(file, WEATHER_DATA_PATH)
            df = add_weather_data(df, weather_file_path)

            # Drop unneeded columns
            df = drop_column(df, file_name)
            df = drop_lower_outliers(df, columns=['trip_duration', 'trip_distance_miles'], lower_quantile=0.05)

            # save to parquet file
            path = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['taxi_folder_name']}/" + file_name
            df.to_parquet(path, index=False, filesystem=s3_fs, engine='pyarrow')
            print("Finished transforming data in file: " + path)
            print("==========================================================================================")

    if NEW_YEAR:
        pattern = str(TAXI_DATA_PATH / NEW_YEAR / "*.parquet")
        files = glob(pattern)
        if not files:
            logger.warning("No parquet files found for new year %s at %s", NEW_YEAR, pattern)
            return
        
        for file in files:
            file_name = file.split('\\')[-1]
            print(f"Reading parquet file: {file_name}")

            df = pd.read_parquet(file, engine='pyarrow')

            df = process(df, file_name)
            df = add_lat_lon_from_lookup(df)
            df = add_midpoint(df)
            df = add_trip_duration(df, unit='seconds', inplace=True)
            df = drop_nulls(df)
            df = drop_invalid_time_rows(df, file_name)

            # Merge weather data
            df = find_nearest_point_id(df)
            df = add_pickup_hour_rounded(df)
            df = drop_invalid_coordinates(df)
            weather_file_path = get_matching_weather_file(file, WEATHER_DATA_PATH)
            df = add_weather_data(df, weather_file_path)

            # Drop unneeded columns
            df = drop_column(df, file_name)
            df = drop_lower_outliers(df, columns=['trip_duration', 'trip_distance_miles'], lower_quantile=0.05)

            # save to parquet file
            path = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['new_taxi_folder_name']}/" + file_name
            df.to_parquet(path, index=False, filesystem=s3_fs, engine='pyarrow')
            print("Finished transforming data in file: " + path)
            print("==========================================================================================")

###############################################

if __name__ == "__main__":
    main()