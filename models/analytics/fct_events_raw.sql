{%- set start_date_day = var("start_date_day", run_started_at.date()) -%}
{%- set end_date_day = var("end_date_day", run_started_at.date() + modules.datetime.timedelta(days=1)) -%}

{{ 
    config(
        materialized='incremental',
    )
}}

with events as (

    select * from {{ ref("stg_events") }} 
    where created_date_day >= '{{ start_date_day }}' and created_date_day < '{{ end_date_day }}'
),

event_types as (

    select * from {{ ref("stg_event_types") }}
),

users as (

    select * from {{ ref("stg_users") }}
),

joined as (

    select 
        events.event_id,
        events.session_id,
        events.user_id,
        events.created_at,
        events.created_date_day,
        event_types.event_type_name,
        users.user_city,
        users.user_country
    from events
    left join event_types
    using(event_type_id)
    left join users
    using(user_id)
),

add_session_identifier_1 as (

    select 
        *,
        event_type_name as source_event_type,
        lead(event_type_name) over (partition by user_id,session_id order by event_id,created_at) as destination_event_type_stg
    from joined
),

add_session_identifier as (

    select 
        *,
        case 
            when destination_event_type_stg is null then 'Drop-off'
            else destination_event_type_stg
        end as destination_event_type
    from add_session_identifier_1
)

select
    event_id,
    session_id,
    user_id,
    created_at,
    created_date_day,
    event_type_name,
    user_city,
    user_country,
    source_event_type,
    destination_event_type
from add_session_identifier
