{%- set start_date_day = var("start_date_day", run_started_at.date()) -%}
{%- set end_date_day = var("end_date_day", run_started_at.date() + modules.datetime.timedelta(days=1)) -%}

{{ 
    config(
        materialized='table',
    )
}}

with events as (

    select * from {{ ref("stg_events") }} 
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

{% for i in range(1,6) %}

with_level_{{i}} as (

    select 
        *,
        lag(
            {% if i == 1 %}
                 event_type_name
            {% else %}
            lag_{{i-1}}
            {% endif%}
            ) over (partition by user_id,session_id order by event_id,created_at) as lag_{{i}},
        lead(
            {% if i == 1 %}
                 event_type_name
            {% else %}
            lead_{{i-1}}
            {% endif%}
            ) over (partition by user_id,session_id order by event_id,created_at) as lead_{{i}}
    from 
        {% if i == 1 %}
        joined
        {% else %}
        with_level_{{i-1}}
        {% endif %}
),
{% endfor %}

transformed as (
    SELECT 
        event_type_name,
        event_type_name as source,
        lead_1 as destination,
        1 as level
    FROM with_level_5
    union all
    SELECT 
    
           event_type_name,
           lead_1 as source,
           lead_2 as destination,
           2 as level

    FROM with_level_5
    union all
    SELECT 
    
           event_type_name,
           lead_2 as source,
           lead_3 as destination,
           3 as level
    FROM with_level_5
    union all
    SELECT event_type_name,
           lead_3 as source,
           lead_4 as destination,
           4 as level
    FROM with_level_5
    union all
    SELECT event_type_name,
           lead_4 as source,
           lead_5 as destination,
           5 as level
    FROM with_level_5
    union all
    SELECT event_type_name,
           lag_1 as source,
           event_type_name as destination,
           -1 as level
    FROM with_level_5
    union all
    SELECT 
    
           event_type_name,
           lag_2 as source,
           lag_1 as destination,
           -2 as level

    FROM with_level_5
    union all
    SELECT 
    
           event_type_name,
           lag_3 as source,
           lag_2 as destination,
           -3 as level
    FROM with_level_5
    union all
    SELECT event_type_name,
           lag_4 as source,
           lag_3 as destination,
           -4 as level
    FROM with_level_5
    union all
    SELECT event_type_name,
           lag_5 as source,
           lag_4 as destination,
           -5 as level
    FROM with_level_5
    )

select * from transformed where source is not null and destination is not null