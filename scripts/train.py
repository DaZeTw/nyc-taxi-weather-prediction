import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, unix_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
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

def get_spark_session(app_name="Train Taxi Model"):
    """Khởi tạo Spark Session với cấu hình Delta Lake và S3/MinIO."""
    # local[1] để giới hạn số luồng CPU, để demo đảm bảo chạy được ta đặt là 1
    # set thành * để dùng toàn bộ nhưng lưu ý nó sẽ dùng toàn bộ luồng của máy (có thể không chạy được các ứng dụng khác)
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[1]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.python.worker.timeout", "120") \
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
        # Cấu hình MinIO/S3 (Nên lấy từ biến môi trường trong thực tế)
        # .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
        # .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
        # .config("spark.hadoop.fs.s3a.endpoint", "YOUR_MINIO_ENDPOINT") \
        # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Thêm packages cần thiết
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
    return spark

def prepare_data(df):
    """Tạo các cột feature cơ bản."""
    # Thêm feature thời gian
    df = df.withColumn("pickup_hour", hour("pickup_time")) \
           .withColumn("pickup_day", dayofweek("pickup_time"))
    
    # Loại bỏ null ở các cột quan trọng nếu cần (dựa trên notebook đã drop trước đó)
    return df

def train(data_path, model_output_path, split_ratio=0.8):
    spark = get_spark_session()
    logger.info(f"Đọc dữ liệu từ: {data_path}")
    
    # Đọc dữ liệu (Parquet hoặc Delta)
    # Nếu đường dẫn là delta: spark.read.format("delta").load(data_path)
    # Dựa trên notebook, bạn đang dùng parquet folder
    df = spark.read.parquet(data_path)
    
    # Tiền xử lý
    df = prepare_data(df)
    
    # --- Time-based Splitting ---
    logger.info("Splitting into Train/Test sets...")
    df = df.withColumn("unix_pickup_time", unix_timestamp("pickup_time"))
    quantile_value = df.approxQuantile("unix_pickup_time", [split_ratio], 0.001)[0]
    
    train_data = df.filter(col("unix_pickup_time") < quantile_value)
    test_data = df.filter(col("unix_pickup_time") >= quantile_value)
    
    # Lấy mẫu nhỏ để demo train nhanh (như notebook) - Trong PROD nên bỏ dòng này hoặc tăng fraction
    train_sample = train_data.sample(fraction=0.05, seed=0)
    test_sample = test_data.sample(fraction=0.05, seed=0)
    
    logger.info(f"Train sample: {train_sample.count()}")
    logger.info(f"Test sample: {test_sample.count()}")

    # --- Pipeline Setup ---
    cat_cols = ['taxi_type', 'pickup_day']  # 'rate_code_id' không ảnh hưởng tới duration
    num_cols = ['trip_distance_miles', 'midpoint_latitude', 'midpoint_longitude', 'pickup_hour', 'congestion_surcharge', \
                'temperature_2m', 'precipitation', 'windspeed_10m', 'pressure_msl']
    
    stages = []
    
    # Xử lý biến phân loại
    for cat_col in cat_cols:
        # handleInvalid="keep" hoặc "skip" để tránh lỗi khi gặp nhãn mới lúc predict
        indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_idx", handleInvalid="keep")
        encoder = OneHotEncoder(inputCols=[f"{cat_col}_idx"], outputCols=[f"{cat_col}_vec"])
        stages += [indexer, encoder]

    # Gom nhóm features
    assembler_inputs = [f"{c}_vec" for c in cat_cols] + num_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="unscaled_features", handleInvalid="skip")
    stages += [assembler]

    # Scaler
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="features", withStd=True, withMean=True)
    stages += [scaler]

    # Model (Random Forest như trong notebook stages += [rf])
    rf = RandomForestRegressor(featuresCol="features", labelCol="trip_duration", numTrees=20, maxDepth=5)
    stages += [rf]

    # Tạo và fit Pipeline
    logger.info("Training Pipeline...")
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(train_sample)
    logger.info("Training Completed!")
    
    # --- Evaluate ---
    logger.info("Evaluating model...")
    predictions = model.transform(test_sample)
    evaluator = RegressionEvaluator(labelCol="trip_duration", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    logger.info(f"Root Mean Squared Error (RMSE) on Test Data: {rmse}")

    # --- Save Model ---
    logger.info(f"Saving model at: {model_output_path}")
    # Ghi đè nếu model đã tồn tại
    model.write().overwrite().save(model_output_path)
    
    spark.stop()

def main():
    """Hàm chính để chạy training."""
    from datetime import datetime
    model_name = datetime.now().strftime("taxi_model_%Y%m%d%H%M%S")
    model_path = f"s3a://{BUCKET_NAME_2}/models/" + model_name
    data_path = f"s3a://{BUCKET_NAME_2}/{datalake_cfg['taxi_folder_name']}/"
    train(data_path, model_path)

if __name__ == "__main__":
    main()