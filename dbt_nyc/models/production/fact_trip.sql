{{ config(materialized = 'table') }}

with trip_staging as (
    select 
        -- identifiers
        {{ dbt_utils.surrogate_key(['f.vendor_id', 'f.rate_code_id', 'f.pu_location_id', 'f.do_location_id', 'f.payment_type', 'f.pickup_datetime', 'f.dropoff_datetime']) }} as trip_id,
        f.vendor_id,
        f.rate_code_id,
        f.pu_location_id,
        f.do_location_id,
        f.payment_type,

        -- timestamps
        f.pickup_datetime,
        f.dropoff_datetime,

        -- trip info
        f.passenger_count,
        f.trip_distance,
        f.store_and_fwd_flag,

        -- payment info
        f.fare_amount,
        f.extra,
        f.mta_tax,
        f.tip_amount,
        f.tolls_amount,
        f.improvement_surcharge,
        f.total_amount,
        f.congestion_surcharge,
        -- f.airport_fee,  ← REMOVED (doesn't exist in staging table)

        -- weather info
        f.temperature_2m,
        f.precipitation,
        f.windspeed_10m,
        f.pressure_msl,

        -- derived features
        f.trip_duration_minutes,
        f.hour_of_day,
        f.day_of_week,
        f.is_weekend,
        f.avg_speed_mph,

        -- partition columns
        f.year,
        f.month,
        f.taxi_type

    from 
        {{ source('staging', 'nyc_taxi_weather') }} as f
    join 
        {{ ref('dim_vendor') }} as dv ON f.vendor_id = dv.vendor_id
    join
        {{ ref('dim_rate_code') }} as dr ON f.rate_code_id = dr.rate_code_id
    join
        {{ ref('dim_payment') }} as dp ON f.payment_type = dp.payment_type
)

select
    *
from 
    trip_staging