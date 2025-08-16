

select
  event_date,
  count(*)::bigint as orders,
  sum(total)        as revenue
from "warehouse"."main_main"."stg_order_placed"
group by event_date
order by event_date