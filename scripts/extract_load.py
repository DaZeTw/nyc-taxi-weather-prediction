import sys
import os
from glob import glob
from minio import Minio
from minio.error import S3Error

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient
###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"
YEARS = ["2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"]
###############################################


###############################################
# Main
###############################################
def extract_load(endpoint_url, access_key, secret_key):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    client.create_bucket(datalake_cfg["bucket_name_1"])
    # Create the connection ONCE here
    client_minio = client.create_conn()

    # ==========================================
    # 1. Upload Taxi Data
    # ==========================================
    print("\n" + "="*60)
    print("🚕 UPLOADING TAXI DATA")
    print("="*60)
    
    for year in YEARS:
        # Construct the search path (assuming data/YEAR/*.parquet)
        search_path = os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet")
        all_fps = glob(search_path)

        if not all_fps:
            print(f"⚠️  No taxi files found for year {year} in {search_path}")
            continue

        for fp in all_fps:
            file_name = os.path.basename(fp)
            # The name it will have inside the bucket
            object_name = os.path.join(datalake_cfg["folder_name"], file_name)

            try:
                # 1. Check if object already exists
                stat = client_minio.stat_object(datalake_cfg["bucket_name_1"], object_name)
                
                # 2. Compare sizes (optional but recommended)
                local_size = os.path.getsize(fp)
                if stat.size == local_size:
                    print(f"✅ Skipping {file_name}: Already exists with correct size")
                    continue
                else:
                    print(f"⚠️  Size mismatch for {file_name}. Re-uploading...")

            except S3Error as e:
                # If the error is 404, it means the file is NOT there
                if e.code == "NoSuchKey":
                    pass 
                else:
                    raise e

            # 3. Upload if it doesn't exist or size is different
            print(f"🚀 Uploading taxi data: {file_name}")
            client_minio.fput_object(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name=object_name,
                file_path=fp,
            )

    # ==========================================
    # 2. Upload Weather Data
    # ==========================================
    print("\n" + "="*60)
    print("🌤️  UPLOADING WEATHER DATA")
    print("="*60)
    
    # Path to weather data folder
    weather_folder = os.path.join("data", "weather")
    weather_search_path = os.path.join(weather_folder, "weather_*.parquet")
    weather_files = glob(weather_search_path)

    if not weather_files:
        print(f"⚠️  No weather files found in {weather_folder}")
    else:
        for fp in weather_files:
            file_name = os.path.basename(fp)
            # Store weather data in a separate folder in MinIO
            object_name = os.path.join("weather", file_name)

            try:
                # 1. Check if object already exists
                stat = client_minio.stat_object(datalake_cfg["bucket_name_1"], object_name)
                
                # 2. Compare sizes
                local_size = os.path.getsize(fp)
                if stat.size == local_size:
                    print(f"✅ Skipping {file_name}: Already exists with correct size")
                    continue
                else:
                    print(f"⚠️  Size mismatch for {file_name}. Re-uploading...")

            except S3Error as e:
                if e.code == "NoSuchKey":
                    pass 
                else:
                    raise e

            # 3. Upload weather file
            print(f"🚀 Uploading weather data: {file_name}")
            client_minio.fput_object(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name=object_name,
                file_path=fp,
            )

    print("\n" + "="*60)
    print("✅ MinIO Synchronization Complete!")
    print("="*60)
###############################################


if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    ENDPOINT_URL_LOCAL = "minio:9000"
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']

    extract_load(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)