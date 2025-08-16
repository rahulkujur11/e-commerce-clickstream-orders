{{ config(materialized='view') }}

select
  cast(event_id as varchar)    as event_id,
  cast(order_id as varchar)    as order_id,
  cast(user_id  as varchar)    as user_id,
  cast(ts       as timestamp)  as ts,
  cast(total    as double)     as total,
  line_items,                      -- DuckDB can read LIST<STRUCT{sku,qty,price}>
  cast(event_date as date)     as event_date,
  cast(ingest_ts  as timestamp) as ingest_ts
from read_parquet('/opt/data/silver/order_placed/**/*.parquet')
