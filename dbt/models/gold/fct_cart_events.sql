{{ config(materialized='view') }}
select
  event_date,
  count(*)::bigint as cart_events,
  count(distinct user_id) as users_carted
from {{ ref('stg_add_to_cart') }}
group by event_date
order by event_date
