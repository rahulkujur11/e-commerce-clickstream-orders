{{ config(materialized='view') }}

with li as (
  select sku, event_date, line_total
  from {{ ref('fct_order_items') }}
),
cat as (
  select sku, category
  from {{ ref('dim_sku') }}   -- seed with (sku, category)
)
select
  li.event_date,
  coalesce(cat.category, 'Unknown') as category,
  sum(li.line_total)                as revenue
from li
left join cat on cat.sku = li.sku
group by 1,2
order by 1,2
