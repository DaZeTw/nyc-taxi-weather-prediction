{{ config(materialized = 'table') }}

with pickup_location as (
    select distinct 
        pu_location_id
    from 
        {{ source('staging', 'nyc_taxi_weather') }}
    where 
        pu_location_id is not null
)

select 
    pu_location_id
from 
    pickup_location
where
    pu_location_id is not null
order by 
    pu_location_id asc