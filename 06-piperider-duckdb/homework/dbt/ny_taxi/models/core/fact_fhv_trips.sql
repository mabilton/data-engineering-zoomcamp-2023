{{ config(materialized='table') }}

with fhv_data as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
    where
        pickup_datetime between '2019-01-01' and '2020-12-31'
    and
        dropoff_datetime between '2019-01-01' and '2020-12-31'
),

zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pickup_locationid,
    dropoff_locationid,
    sr_flag,
    affiliated_base_num,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from fhv_data
inner join zones as pickup_zone
    on fhv_data.pickup_locationid = pickup_zone.locationid
inner join zones as dropoff_zone
    on fhv_data.dropoff_locationid = dropoff_zone.locationid
