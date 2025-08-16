
select
  event_date,
  count(*)::bigint as cart_events,
  count(distinct user_id) as users_carted
from "warehouse"."main_main"."stg_add_to_cart"
group by event_date
order by event_date