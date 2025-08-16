

with li as (
  select event_date, qty, line_total
  from "warehouse"."main_main"."fct_order_items"
)
select
  event_date,
  sum(line_total)                 as revenue,
  sum(qty)                        as units_sold
from li
group by 1
order by 1