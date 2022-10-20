{%- set start_date_day = var("start_date_day", run_started_at.date()) -%}
{%- set end_date_day = var("end_date_day", run_started_at.date() + modules.datetime.timedelta(days=1)) -%}

{{ 
    config(
        materialized='incremental',
        unique_key = 'event_id'
    )
}}

with events as (

    select * from {{ ref("fct_events_raw") }} 
    where created_date_day >= '{{ start_date_day }}' and created_date_day < '{{ end_date_day }}'
),

aggregated as (

    select
        created_date_day,
        user_country,
        user_city,
        source_event_type,
        destination_event_type,
        count(*) as number_of_users
    from events
    group by 1,2,3,4,5
)

select 
    created_date_day,
    user_country,
    user_city,
    source_event_type,
    destination_event_type,
    number_of_users
from aggregated