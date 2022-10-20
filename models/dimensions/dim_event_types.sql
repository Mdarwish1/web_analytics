{{ 
    config(
        materialized='table',
    )
}}

select
    event_type_id::int,
    event_type_name::varchar(max)
from {{ ref("stg_event_types") }}