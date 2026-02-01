{{ config(materialized = 'table') }}

with vendor_staging as (
    select distinct
        vendor_id
    from 
        {{ source('staging', 'nyc_taxi_weather') }}
    where 
        vendor_id is not null
)

select 
    vendor_id,
    {{ get_vendor_description('vendor_id') }} as vendor_name
from 
    vendor_staging
where
    vendor_id is not null
    and
    vendor_id in (1, 2)
order by
    vendor_id asc