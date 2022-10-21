{%- set max_level = var("max_level",5) -%}
{%- set min_level = var("min_level",-5) -%}
{%- set event_type = var("event_type") -%}

{{ 
    config(
        materialized='view',
    )
}}

select 
    event_type_name,
    source,
    destination,
    count(*) as users 
from {{ ref("fct_highlighted_events") }} 
where event_type_name = '{{event_type}}' and level >= {{ min_level }} and level <= {{ max_level }}
group by 1,2,3
