
  
  create view "warehouse"."main_main"."stg_sessions__dbt_tmp" as (
    

with events as (
  select user_id, ts, 'page_view'    as event_type from "warehouse"."main_main"."stg_page_view"
  union all
  select user_id, ts, 'add_to_cart'  as event_type from "warehouse"."main_main"."stg_add_to_cart"
  union all
  select user_id, ts, 'order_placed' as event_type from "warehouse"."main_main"."stg_order_placed"
),
ordered as (
  select
    user_id,
    ts,
    event_type,
    lag(ts) over (partition by user_id order by ts) as prev_ts
  from events
),
marks as (
  select
    *,
    case when prev_ts is null or ts - prev_ts > interval '30 minutes' then 1 else 0 end as is_new_session
  from ordered
),
grp as (
  select
    *,
    sum(is_new_session) over (partition by user_id order by ts rows unbounded preceding) as session_num
  from marks
)
select
  user_id || '|' || strftime('%Y-%m-%dT%H:%M:%S', min(ts)) || '|' || strftime('%Y-%m-%dT%H:%M:%S', max(ts)) as session_id,
  user_id,
  min(ts) as session_start,
  max(ts) as session_end,
  sum(case when event_type='page_view' then 1 else 0 end) as page_count,
  sum(case when event_type='add_to_cart' then 1 else 0 end) as cart_events,
  max(case when event_type='order_placed' then 1 else 0 end) as conversion_flag,
  cast(min(ts) as date) as session_date,
  now() as ingest_ts
from grp
group by user_id, session_num
  );
