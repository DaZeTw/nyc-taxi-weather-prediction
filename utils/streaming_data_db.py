import os
import sys
from time import sleep
from pyarrow.parquet import ParquetFile
import pyarrow as pa 
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(".env")

from postgresql_client import PostgresSQLClient

###############################################
# Parameters & Arguments
###############################################
TABLE_NAME = "iot.taxi_nyc_time_series"
PARQUET_FILE = "./data/2025/yellow_tripdata_2025-01.parquet"
NUM_ROWS = 100  # Stream 100 rows at a time
SLEEP_SECONDS = 1  # Delay between inserts
###############################################


###############################################
# Main
###############################################
def main():

    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB", "nyc_taxi_db"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )

    # Get all columns from the table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(f"✅ Table columns: {columns}")
    except Exception as e:
        print(f"❌ Failed to get schema for table with error: {e}")
        return

    # Read parquet file
    pf = ParquetFile(PARQUET_FILE) 
    first_n_rows = next(pf.iter_batches(batch_size=NUM_ROWS)) 
    df = pa.Table.from_batches([first_n_rows]).to_pandas()
    
    print(f"📊 Loaded {len(df)} rows from parquet")
    print(f"📋 DataFrame columns: {df.columns.tolist()}")

    # Stream data row by row
    print(f"\n🚀 Starting to stream data to {TABLE_NAME}...")
    print("="*100)
    
    for idx, row in df.iterrows():
        try:
            # Map parquet columns to database schema
            record = map_to_schema(row)
            
            # Build insert query matching iot.taxi_nyc_time_series schema
            query = f"""
                INSERT INTO {TABLE_NAME} (
                    vendor_id, pickup_datetime, dropoff_datetime,
                    passenger_count, trip_distance, rate_code_id, store_and_fwd_flag,
                    pu_location_id, do_location_id, payment_type,
                    fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
                    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
                ) VALUES (
                    {record['vendor_id']}, '{record['pickup_datetime']}', '{record['dropoff_datetime']}',
                    {record['passenger_count']}, {record['trip_distance']}, {record['rate_code_id']}, '{record['store_and_fwd_flag']}',
                    {record['pu_location_id']}, {record['do_location_id']}, {record['payment_type']},
                    {record['fare_amount']}, {record['extra']}, {record['mta_tax']}, {record['tip_amount']}, {record['tolls_amount']},
                    {record['improvement_surcharge']}, {record['total_amount']}, {record['congestion_surcharge']}, {record['airport_fee']}
                )
            """
            
            pc.execute_query(query)
            print(f"✅ [{idx+1}/{len(df)}] Sent: {format_record(record)}")
            print("-"*100)
            
            sleep(SLEEP_SECONDS)
            
        except Exception as e:
            print(f"❌ Error inserting row {idx}: {e}")
            continue

    print(f"\n🎉 Streaming completed! Inserted {len(df)} records")


def map_to_schema(row):
    """
    Map parquet row to iot.taxi_nyc_time_series schema
    """
    # Extract datetime components
    pickup_dt = row.get('tpep_pickup_datetime', row.get('lpep_pickup_datetime'))
    dropoff_dt = row.get('tpep_dropoff_datetime', row.get('lpep_dropoff_datetime'))
    
    # Convert to datetime if string
    if isinstance(pickup_dt, str):
        pickup_dt = datetime.fromisoformat(pickup_dt.replace('Z', '+00:00'))
    if isinstance(dropoff_dt, str):
        dropoff_dt = datetime.fromisoformat(dropoff_dt.replace('Z', '+00:00'))
    
    return {
        'vendor_id': int(row.get('VendorID', 1)),
        'pickup_datetime': pickup_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'dropoff_datetime': dropoff_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'passenger_count': int(row.get('passenger_count', 1)),
        'trip_distance': float(row.get('trip_distance', 0)),
        'rate_code_id': int(row.get('RatecodeID', 1)),
        'store_and_fwd_flag': str(row.get('store_and_fwd_flag', 'N')),
        'pu_location_id': int(row.get('PULocationID', 1)),
        'do_location_id': int(row.get('DOLocationID', 1)),
        'payment_type': int(row.get('payment_type', 1)),
        'fare_amount': float(row.get('fare_amount', 0)),
        'extra': float(row.get('extra', 0)),
        'mta_tax': float(row.get('mta_tax', 0)),
        'tip_amount': float(row.get('tip_amount', 0)),
        'tolls_amount': float(row.get('tolls_amount', 0)),
        'improvement_surcharge': float(row.get('improvement_surcharge', 0)),
        'total_amount': float(row.get('total_amount', 0)),
        'congestion_surcharge': float(row.get('congestion_surcharge', 0)),
        'airport_fee': float(row.get('airport_fee', 0))
    }


def format_record(record):
    """
    Format record for logging
    """
    return {
        'vendor': record['vendor_id'],
        'pickup': record['pickup_datetime'],
        'dropoff': record['dropoff_datetime'],
        'distance': f"{record['trip_distance']:.2f} mi",
        'fare': f"${record['total_amount']:.2f}",
        'passengers': record['passenger_count']
    }


###############################################


if __name__ == "__main__":
    main()