{{ 
    config(
        materialized='view',
    )
}}

with source as (

    select 
        id as event_type_id,
        name as event_type_name
    from {{ source('web_analytics','event_types') }}
)

select
    event_type_id::int,
    event_type_name::varchar(max)
from source
