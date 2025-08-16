{{ config(materialized='view') }}

with pv as (
  select event_date, count(*)::bigint as page_views
  from {{ ref('stg_page_view') }} group by event_date
),
cart as (
  select event_date, count(*)::bigint as add_to_carts
  from {{ ref('stg_add_to_cart') }} group by event_date
),
ord as (
  select event_date, count(*)::bigint as orders
  from {{ ref('stg_order_placed') }} group by event_date
)
select
  coalesce(pv.event_date, cart.event_date, ord.event_date) as event_date,
  pv.page_views, cart.add_to_carts, ord.orders,
  case when pv.page_views > 0 then ord.orders::double / pv.page_views else null end as view_to_order_cr,
  case when cart.add_to_carts > 0 then ord.orders::double / cart.add_to_carts else null end as cart_to_order_cr
from pv
full join cart using (event_date)
full join ord  using (event_date)
order by event_date
