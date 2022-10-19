{{ 
    config(
        materialized='view',
    )
}}

with source as (

    select 
        id as user_id,
        name as user_name,
        date as created_at,
        email as user_email,
        country as user_country,
        city as user_city,
        phone as user_phone_number
    from {{ source('web_analytics','users') }}
)

select
    user_id,
    user_name,
    created_at,
    user_email,
    user_country,
    user_city,
    user_phone_number
from source
