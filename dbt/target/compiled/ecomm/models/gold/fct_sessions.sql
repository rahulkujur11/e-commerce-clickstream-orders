
select
  session_date,
  count(*)::bigint as sessions,
  sum(page_count) as total_pages,
  sum(cart_events) as total_cart_events,
  sum(conversion_flag) as conversions
from "warehouse"."main_main"."stg_sessions"
group by session_date
order by session_date