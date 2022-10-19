{{ 
    config(
        materialized='table',
    )
}}

with source as (

    select 
        id as event_id,
        event_type_id,
        session_id,
        user_id,
        created_at,
        cast(created_at as date) as created_date_day 
    from {{ source('web_analytics','events') }}
)

select
    event_id::int,
    event_type_id::int,
    session_id::varchar(32),
    user_id::int,
    created_at::datetime,
    created_date_day::date
from source
