{{ 
    config(
        materialized='table',
    )
}}

select
    country_iso_code::varchar(3),
    country_name::varchar(max)
from {{ ref("stg_countries") }}