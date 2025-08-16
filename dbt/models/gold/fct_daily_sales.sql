{{ config(materialized='view') }}

select
  event_date,
  count(*)::bigint as orders,
  sum(total)        as revenue
from {{ ref('stg_order_placed') }}
group by event_date
order by event_date
