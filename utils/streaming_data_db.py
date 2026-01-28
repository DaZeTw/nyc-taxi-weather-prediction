import os
import sys
from time import sleep
from pyarrow.parquet import ParquetFile
import pyarrow as pa 

from dotenv import load_dotenv
load_dotenv(".env")

from postgresql_client import PostgresSQLClient

###############################################
# Parameters & Arguments
###############################################
TABLE_NAME = "iot.taxi_nyc_time_series"
PARQUET_FILE = "./data/yellow_tripdata_2024-01.parquet"
NUM_ROWS = 10000
###############################################


###############################################
# Main
###############################################
def main():

    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )

    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")

    # Loop over all columns and create random values
    pf = ParquetFile(PARQUET_FILE) 
    first_n_rows = next(pf.iter_batches(batch_size = NUM_ROWS)) 
    df = pa.Table.from_batches([first_n_rows]).to_pandas()
    
    # Debug: print actual column names
    print("DataFrame columns:", df.columns.tolist())
    
    # Convert datetime columns to string - use actual column names from parquet
    if 'lpep_pickup_datetime' in df.columns:
        df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(dtype='str')
    if 'lpep_dropoff_datetime' in df.columns:
        df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(dtype='str')

    for _, row in df.iterrows():

        # Insert data
        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(row)}
        """
        print(f"Sent: {format_record(row)}")
        pc.execute_query(query)
        print("-"*100)
        sleep(2)

def format_record(row):
    taxi_res = {
        'VendorID': row['VendorID'],
        'RatecodeID': row['RatecodeID'],
        'DOLocationID': row['DOLocationID'],
        'PULocationID': row['PULocationID'],
        'payment_type': row['payment_type'],
        'lpep_dropoff_datetime': str(row['lpep_dropoff_datetime']),
        'lpep_pickup_datetime': str(row['lpep_pickup_datetime']),
        'passenger_count': row['passenger_count'],
        'trip_distance': row['trip_distance'],
        'extra': row['extra'],
        'mta_tax': row['mta_tax'],
        'fare_amount': row['fare_amount'],
        'tip_amount': row['tip_amount'],
        'tolls_amount': row['tolls_amount'],
        'total_amount': row['total_amount'],
        'improvement_surcharge': row['improvement_surcharge'],
        'congestion_surcharge': row['congestion_surcharge'],
    }
    return taxi_res
###############################################


if __name__ == "__main__":
    main()