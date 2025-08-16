{{ config(materialized='table') }}

with li as (
  select sku, qty, price, line_total, event_date
  from {{ ref('fct_order_items') }}
),
agg as (
  select
    sku,
    min(event_date)         as first_sold_date,
    sum(qty)                as units_sold,
    sum(line_total)         as gross_revenue,
    avg(price)              as avg_unit_price
  from li
  group by 1
),
cat as (
  select sku, category
  from {{ ref('dim_sku') }}
)
select
  a.sku,
  coalesce(c.category, 'Unknown') as category,
  a.first_sold_date,
  a.units_sold,
  a.gross_revenue,
  a.avg_unit_price
from agg a
left join cat c on c.sku = a.sku
