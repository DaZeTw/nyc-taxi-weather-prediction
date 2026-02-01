{{ config(materialized = 'table') }}

with taxi_type_staging as (
    select distinct
        taxi_type
    from 
        {{ source('staging', 'nyc_taxi_weather') }}
    where 
        taxi_type is not null
)

select 
    taxi_type as taxi_type_id,
    {{ get_taxi_type_name('taxi_type') }} as taxi_type_name
from 
    taxi_type_staging
where 
    taxi_type is not null
order by
    taxi_type asc