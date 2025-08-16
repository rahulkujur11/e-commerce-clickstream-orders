{{ config(materialized='view') }}

with orders as (
  select event_id, order_id, user_id, ts, event_date, line_items
  from {{ ref('stg_order_placed') }}
)
select
  o.event_id,
  o.order_id,
  o.user_id,
  o.ts,
  o.event_date,
  u.li.sku      as sku,
  u.li.qty      as qty,
  u.li.price    as price,
  u.li.qty * u.li.price as line_total
from orders o
cross join unnest(o.line_items) as u(li)   -- u = table alias, li = column alias (STRUCT)
