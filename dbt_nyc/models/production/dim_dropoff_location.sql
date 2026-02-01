{{ config(materialized = 'table') }}

with dropoff_location as (
    select distinct 
        do_location_id
    from 
        {{ source('staging', 'nyc_taxi_weather') }}
    where 
        do_location_id is not null
)

select 
    do_location_id
from 
    dropoff_location
where
    do_location_id is not null
order by 
    do_location_id asc