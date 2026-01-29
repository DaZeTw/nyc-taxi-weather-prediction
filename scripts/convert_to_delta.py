import sys
import os
import warnings
import traceback
import logging
import time
from minio import Minio

from pyspark import SparkConf, SparkContext

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)

# os.environ['HADOOP_HOME'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop'))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop', 'bin')))
# Định nghĩa đường dẫn Hadoop
hadoop_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop'))
os.environ['HADOOP_HOME'] = hadoop_home
# Thêm bin vào PATH của hệ thống để Java tìm thấy winutils.exe
os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + ";" + os.environ['PATH']

from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"

cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME_2 = datalake_cfg['bucket_name_2']
BUCKET_NAME_3 = datalake_cfg['bucket_name_3']
###############################################


###############################################
# PySpark
###############################################
def delta_convert(endpoint_url, access_key, secret_key):
    """
        Convert parquet file to delta format
    """
    from pyspark.sql import SparkSession
    from delta.pip_utils import configure_spark_with_delta_pip

    jars_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'jars'))
    jars = f"{jars_path}/hadoop-aws-3.3.4.jar,{jars_path}/aws-java-sdk-bundle-1.12.262.jar"
    # jars = "../jars/hadoop-aws-3.3.4.jar,../jars/aws-java-sdk-bundle-1.12.262.jar"

    builder = SparkSession.builder \
                    .appName("Converting to Delta Lake") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.hadoop.fs.s3a.access.key", access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
                    .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                    # .config("spark.jars", jars)
        
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()
    # spark.sparkContext.setLogLevel("DEBUG")
    # spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logging.info('Spark session successfully created!')


    # Create bucket 'delta'
    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )
    client.create_bucket(BUCKET_NAME_3)

    # START_FROM_FILE = "batch/yellow_tripdata_2022-03.parquet"
    # is_first_file = False
    # Convert to delta
    # for file in client.list_parquet_files(BUCKET_NAME_2, prefix=datalake_cfg['folder_name']):
    #     file = file.split('\\')[-1]
    #     print('[DEBUG] file:', file)
    #     path_read = f"s3a://{BUCKET_NAME_2}/" + file
    #     logging.info(f"Reading parquet file: {file}")

    #     # Nếu tên file nhỏ hơn file cần chạy -> Bỏ qua (Skip)
    #     # if file == START_FROM_FILE:
    #     #     is_first_file = True
    #     # if not is_first_file:
    #     #     print(f"[INFO] Skipping processed file: {file}")
    #     #     continue

    #     df = spark.read.parquet(path_read)

    #     # Save to bucket 'delta' 
    #     path_read = f"s3a://{BUCKET_NAME_3}/" + file
    #     logging.info(f"Saving delta file: {file}")
    #     logging.info(f"path_read: {path_read}")

    #     # Khi có thay đổi schema, cần thêm option("overwriteSchema", "true")
    #     df_delta = df.write \
    #                 .format("delta") \
    #                 .mode("overwrite") \
    #                 .option("overwriteSchema", "true") \
    #                 .save(f"s3a://{BUCKET_NAME_3}/{datalake_cfg['folder_name']}")
        
    #     logging.info("="*50 + "COMPLETED" + "="*50)

    # Đọc toàn bộ folder chứa parquet (Spark tự đệ quy tìm file)
    source_path = f"s3a://{BUCKET_NAME_2}/{datalake_cfg['folder_name']}"
    df = spark.read.parquet(source_path)
    logging.info(f"Reading all parquet files from: {source_path}")

    # Ghi 1 lần duy nhất sang Delta
    target_path = f"s3a://{BUCKET_NAME_3}/{datalake_cfg['folder_name']}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    logging.info("="*50 + "COMPLETED" + "="*50)

    path_read = f"s3a://{BUCKET_NAME_2}/{datalake_cfg['folder_name']}"
    logging.info(f"Reading all parquet files from: {path_read}")

    logging.info("Stopping Spark Session...")
    spark.stop() # <--- QUAN TRỌNG

    import time
    time.sleep(5)
###############################################


###############################################
# Main
###############################################
if __name__ == "__main__":
    delta_convert(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
###############################################
