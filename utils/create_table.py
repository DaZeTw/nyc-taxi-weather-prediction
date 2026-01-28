import os

from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv(".env")


def main():

    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )

    # ==========================================
    # IOT Table: Real-time taxi data
    # ==========================================
    create_table_iot = """
        CREATE TABLE IF NOT EXISTS iot.taxi_nyc_time_series( 
            vendor_id               INT, 
            pickup_datetime         TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime        TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count         FLOAT, 
            trip_distance           FLOAT, 
            rate_code_id            FLOAT, 
            store_and_fwd_flag      VARCHAR, 
            pu_location_id          INT, 
            do_location_id          INT, 
            payment_type            INT, 
            fare_amount             FLOAT, 
            extra                   FLOAT, 
            mta_tax                 FLOAT, 
            tip_amount              FLOAT, 
            tolls_amount            FLOAT, 
            improvement_surcharge   FLOAT, 
            total_amount            FLOAT, 
            congestion_surcharge    FLOAT, 
            airport_fee             FLOAT
        );
    """

    # ==========================================
    # STAGING Table: Batch data with weather (validated)
    # ==========================================
    create_table_staging = """
        CREATE TABLE IF NOT EXISTS staging.nyc_taxi_weather (
            -- Partition columns
            year                    INT,
            month                   INT,
            
            -- Taxi identifiers
            taxi_type               VARCHAR(10),
            vendor_id               INT,
            
            -- Trip timestamps
            pickup_datetime         TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime        TIMESTAMP WITHOUT TIME ZONE,
            
            -- Trip details
            passenger_count         INT, 
            trip_distance           FLOAT,
            rate_code_id            INT,
            store_and_fwd_flag      VARCHAR(1),
            
            -- Location
            pu_location_id          INT, 
            do_location_id          INT,
            
            -- Payment
            payment_type            INT,
            fare_amount             FLOAT, 
            extra                   FLOAT, 
            mta_tax                 FLOAT, 
            tip_amount              FLOAT, 
            tolls_amount            FLOAT, 
            total_amount            FLOAT, 
            improvement_surcharge   FLOAT, 
            congestion_surcharge    FLOAT,
            
            -- Weather data
            temperature_2m          FLOAT,
            precipitation           FLOAT,
            windspeed_10m           FLOAT,
            pressure_msl            FLOAT,
            
            -- Derived features (if from Delta)
            trip_duration_minutes   FLOAT,
            hour_of_day             INT,
            day_of_week             INT,
            is_weekend              INT,
            avg_speed_mph           FLOAT
        );
    """

    # ==========================================
    # PRODUCTION Table: Gold layer for ML/Analytics
    # ==========================================
    create_table_production = """
        CREATE TABLE IF NOT EXISTS production.taxi_ml_features (
            -- Partition columns
            year                    INT,
            month                   INT,
            
            -- Identifiers
            taxi_type               VARCHAR(10),
            vendor_id               INT,
            
            -- Timestamps
            pickup_datetime         TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime        TIMESTAMP WITHOUT TIME ZONE,
            
            -- Trip features
            passenger_count         INT, 
            trip_distance           FLOAT,
            trip_duration_minutes   FLOAT,
            avg_speed_mph           FLOAT,
            
            -- Location features
            pu_location_id          INT, 
            do_location_id          INT,
            rate_code_id            INT,
            
            -- Temporal features
            hour_of_day             INT,
            day_of_week             INT,
            is_weekend              INT,
            
            -- Payment features
            payment_type            INT,
            fare_amount             FLOAT,
            tip_amount              FLOAT,
            total_amount            FLOAT,
            
            -- Weather features
            temperature_2m          FLOAT,
            precipitation           FLOAT,
            windspeed_10m           FLOAT,
            pressure_msl            FLOAT,
            
            -- Target variable (for ML)
            trip_duration_actual    FLOAT
        );
    """

    try:
        print("🔧 Creating tables...")
        
        pc.execute_query(create_table_iot)
        print("✅ Created: iot.taxi_nyc_time_series")
        
        pc.execute_query(create_table_staging)
        print("✅ Created: staging.nyc_taxi_weather")
        
        # pc.execute_query(create_table_production)
        # print("✅ Created: production.taxi_ml_features")
        
        print("\n🎉 All tables created successfully!")
        
    except Exception as e:
        print(f"❌ Failed to create table with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()