{{ 
    config(
        materialized='table',
    )
}}

select
    user_id::int,
    user_name::varchar(max),
    created_at::datetime,
    user_email::varchar(max),
    user_country::varchar(max),
    user_city::varchar(max),
    user_phone_number::varchar(max)
from {{ ref("stg_users") }}