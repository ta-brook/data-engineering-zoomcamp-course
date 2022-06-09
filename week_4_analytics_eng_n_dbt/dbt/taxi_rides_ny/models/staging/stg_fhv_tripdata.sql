{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'Affiliated_base_number']) }} as tripid,
    dispatching_base_num,	
    Affiliated_base_number,
    
    -- timestamps
    pickup_datetime,	
    dropOff_datetime,	
    PUlocationID as pickup_locationid,
    DOlocationID as dropoff_locationid,
    SR_Flag as sr_flag,
    
from 
    tripdata
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}