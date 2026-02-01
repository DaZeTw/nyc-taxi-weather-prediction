{{ config(materialized = 'table') }}

with rate_code_staging as (
    select distinct 
        rate_code_id
    from 
        {{ source('staging', 'nyc_taxi_weather') }}  -- ✅ Changed from staging.nyc_taxi
    where 
        rate_code_id is not null
)

select 
    rate_code_id,
    {{ get_rate_code_description('rate_code_id') }} as rate_code_description
from 
    rate_code_staging
where 
    rate_code_id is not null
    and 
    rate_code_id in (1, 2, 3, 4, 5, 6)
order by
    rate_code_id asc