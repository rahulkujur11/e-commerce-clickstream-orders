{{ config(materialized='view') }}

with sess as (
  select user_id, session_start, session_end, conversion_flag
  from {{ ref('stg_sessions') }}
),
orders as (
  select user_id, total
  from {{ ref('stg_order_placed') }}
),
agg as (
  select
    user_id,
    min(session_start) as first_seen_at,
    max(session_end)   as last_seen_at,
    count(*)           as total_sessions,
    sum(conversion_flag) as converting_sessions
  from sess
  group by 1
),
ord as (
  select
    user_id,
    count(*)    as total_orders,
    sum(total)  as lifetime_value
  from orders
  group by 1
)
select
  a.user_id,
  a.first_seen_at,
  a.last_seen_at,
  a.total_sessions,
  coalesce(o.total_orders, 0)    as total_orders,
  coalesce(o.lifetime_value, 0)  as lifetime_value,
  a.converting_sessions
from agg a
left join ord o on a.user_id = o.user_id
