import sys
import os
import logging
import boto3

# Ép buộc Spark sử dụng đúng python.exe trong môi trường Anaconda hiện tại
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek
from pyspark.ml import PipelineModel
from delta import configure_spark_with_delta_pip

###############################################
# Parameters & Arguments
##############################################
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent 
UTILS_PATH = BASE_DIR.parent / "utils"              
sys.path.append(str(UTILS_PATH))

from helpers import load_cfg
CFG_FILE = "./config/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']
###############################################

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name="Predict Taxi Model", memory="4g"):
    """Khởi tạo Spark Session."""
    builder = SparkSession.builder \
            .appName(app_name) \
            .master("local[1]") \
            .config("spark.driver.memory", memory) \
            .config("spark.executor.memory", memory) \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "2g") \
            .config("spark.python.worker.timeout", "120") \
            .config("spark.python.worker.reuse", "false") \
            .config("spark.local.dir", "D:/Big Data/NYC_Taxi_Data_Pipeline/spark_temp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
    return spark

def predict(spark, data_path, model_path, output_path):
    # 1. Load Model
    logger.info(f"Loading model from: {model_path}")
    model = PipelineModel.load(model_path)
    
    # 2. Đọc dữ liệu cần predict
    logger.info(f"Reading inference data from: {data_path}")
    df = spark.read.parquet(data_path) # Hoặc .format("delta").load(data_path)
    
    # 3. Feature Engineering (Bắt buộc phải khớp với bước prepare_data ở train)
    # Pipeline Model chỉ chứa logic biến đổi (Scaler/Encoder), 
    # nhưng các cột đầu vào (pickup_hour...) phải được tạo ra trước khi đưa vào pipeline.
    logger.info("Creating new features (Hour, Day)...")
    df = df.withColumn("pickup_hour", hour("pickup_time")) \
           .withColumn("pickup_day", dayofweek("pickup_time"))
    
    # 4. Dự đoán
    logger.info("Performing prediction...")
    predictions = model.transform(df)
    
    # 5. Chọn cột kết quả
    result_df = predictions.select(
        "pickup_time", 
        "trip_distance_miles",
        "trip_duration", # Giá trị thực tế (nếu có để so sánh)
        col("prediction").alias("predicted_duration")
    )
    
    # 6. Lưu kết quả
    logger.info(f"Saving prediction results to: {output_path}")
    # Lưu dưới dạng Delta hoặc Parquet
    result_df.write.format("parquet").mode("overwrite").save(output_path)

def predict_single_record(spark,
                          model, 
                          pickup_time, 
                          trip_distance,
                          distance_unit,  
                          midpoint_latitude, 
                          midpoint_longitude,
                          temperature_2m,
                          precipitation,
                          windspeed_10m,
                          pressure_msl
                          ):
    """Hàm phụ trợ để dự đoán một bản ghi đơn lẻ (nếu cần)."""
    from pyspark.sql import Row

    # Chuyển đổi đơn vị khoảng cách nếu cần
    if distance_unit == "km":
        trip_distance = trip_distance * 0.621371

    # Tạo DataFrame từ bản ghi đơn
    data = [Row(taxi_type="yellow",  # Giả sử taxi_type là 'yellow'
                pickup_time=pickup_time,
                trip_distance_miles=trip_distance,
                midpoint_latitude=midpoint_latitude,
                midpoint_longitude=midpoint_longitude,
                congestion_surcharge=0.0,  # Giả sử không có congestion, vì cần thông tin traffic real-time
                temperature_2m=temperature_2m,
                precipitation=precipitation,
                windspeed_10m=windspeed_10m,
                pressure_msl=pressure_msl
                )]
    df = spark.createDataFrame(data)

    # Tạo các cột feature
    df = df.withColumn("pickup_hour", hour("pickup_time"))\
           .withColumn("pickup_day", dayofweek("pickup_time"))
    
    # Dự đoán
    prediction = model.transform(df)
    predicted_duration = prediction.select("prediction").collect()[0][0]

    return predicted_duration


# --- Helper Function (Đặt ngoài if __main__) ---
def get_boto3_client():
    """Hàm phụ trợ để tạo boto3 client chuẩn, tránh lặp code"""
    # 1. Lấy endpoint gốc từ config
    raw_endpoint = MINIO_ENDPOINT
    
    # 2. Ép buộc thêm http:// nếu thiếu (FIX LỖI INVALID ENDPOINT)
    if not raw_endpoint.startswith("http"):
        final_endpoint = f"http://{raw_endpoint}"
    else:
        final_endpoint = raw_endpoint

    # 3. Trả về client đã cấu hình chuẩn
    return boto3.client(
        's3',
        endpoint_url=final_endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=False
    )

def get_latest_data_file(bucket, prefix, suffix=""):
    """Trả về full path của object mới nhất trong bucket theo alphabet"""
    s3 = get_boto3_client()
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
    # Sort theo Key để lấy file có ngày tháng lớn nhất (vd: 2025-02 > 2025-01)
    candidates = sorted([o['Key'] for o in objs if o['Key'].endswith(suffix)])
    return f"s3a://{bucket}/{candidates[-1]}" if candidates else None

def get_latest_model_folder(bucket=BUCKET_NAME_2, prefix="models/"):
    """
    Tìm thư mục model mới nhất dựa trên tên 'taxi_model_YYYYMMDDHHMMSS'.
    Sử dụng Delimiter='/' để S3 trả về danh sách folder (CommonPrefixes).
    """
    s3 = get_boto3_client()
    
    # Đảm bảo prefix có dấu / ở cuối (ví dụ: 'models/')
    if not prefix.endswith('/'):
        prefix += '/'
        
    # List objects với Delimiter để lấy danh sách folder con
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    
    # Lấy danh sách các folder con (CommonPrefixes)
    # Kết quả sẽ dạng: [{'Prefix': 'models/taxi_model_20240101.../'}, ...]
    sub_folders = response.get('CommonPrefixes', [])
    
    if not sub_folders:
        return None
        
    # Lấy ra đường dẫn string và lọc đúng format tên model của bạn
    candidates = [x['Prefix'] for x in sub_folders if "taxi_model_" in x['Prefix']]
    
    if not candidates:
        return None

    # Sort string: Vì format là YYYYMMDDHHMMSS nên cái cuối cùng là cái mới nhất
    latest_folder = sorted(candidates)[-1]
    
    # Xóa dấu '/' ở cuối nếu cần thiết để path đẹp hơn, nhưng Spark thường không quan trọng
    return f"s3a://{bucket}/{latest_folder}"

def main():
    """Hàm main để chạy quá trình dự đoán."""
    BUCKET_NAME = BUCKET_NAME_2
    MODEL_ROOT_FOLDER = "models/"
    DATA_ROOT_FOLDER = f"{datalake_cfg['new_taxi_folder_name']}/"
    
    # 1. Tự động tìm Model mới nhất (Folder)
    # Output ví dụ: s3://my-bucket/models/taxi_model_20250128120000
    model_path = get_latest_model_folder(BUCKET_NAME, MODEL_ROOT_FOLDER)

    # 2. Tự động tìm Data mới nhất (File Parquet) để predict
    # Output ví dụ: s3://my-bucket/taxi_data/yellow_tripdata_2025-01.parquet
    data_path = get_latest_data_file(BUCKET_NAME, DATA_ROOT_FOLDER)

    # 3. Output path (Folder chứa kết quả predict)
    # Nên đặt theo tên model hoặc ngày chạy để dễ tracking
    from datetime import datetime
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"s3a://{BUCKET_NAME}/predictions/run_{run_id}/"

    print(f"--- PREDICTION CONFIG ---")
    print(f"Model Path: {model_path}")
    print(f"Data Path : {data_path}")
    print(f"Output    : {output_path}")

    if model_path and data_path:
        # Gọi hàm predict của bạn ở đây
        spark = get_spark_session()
        predict(spark, data_path, model_path, output_path)
        spark.stop()
    else:
        print("Error: Không tìm thấy Model hoặc Data phù hợp!")

# --- Main Block ---
if __name__ == "__main__":
    main()