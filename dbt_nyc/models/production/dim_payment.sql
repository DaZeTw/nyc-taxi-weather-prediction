{{ config(materialized = 'table') }}

with payment_staging as (
    select distinct 
        payment_type
    from 
        {{ source('staging', 'nyc_taxi_weather') }}
    where 
        payment_type is not null
)

select 
    payment_type,
    {{ get_payment_description('payment_type') }} as payment_description
from 
    payment_staging
where
    payment_type is not null
    and
    payment_type in (1, 2, 3, 4, 5, 6)
order by
    payment_type asc