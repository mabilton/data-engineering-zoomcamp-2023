{{ config(materialized='view') }}

select
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    cast((sr_flag is not null) as boolean) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_num
from {{ source('staging','raw_fhv_tripdata') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

    limit 100

{% endif %}
