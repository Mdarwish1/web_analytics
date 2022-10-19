{{ 
    config(
        materialized='view',
    )
}}

with source as (

    select 
        isocode as country_iso_code,
        name as country_name
    from {{ source('web_analytics','countries') }}
)

select
    country_iso_code::varchar(3),
    country_name::varchar(max)
from source
