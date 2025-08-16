
  
  create view "warehouse"."main_main"."stg_add_to_cart__dbt_tmp" as (
    

select
  cast(event_id as varchar)    as event_id,
  cast(user_id  as varchar)    as user_id,
  cast(ts       as timestamp)  as ts,
  sku,
  cast(qty   as integer)       as qty,
  cast(price as double)        as price,
  cast(qty * price as double)  as line_total,
  cast(event_date as date)     as event_date,
  cast(ingest_ts  as timestamp) as ingest_ts
from read_parquet('/opt/data/silver/add_to_cart/**/*.parquet')
  );
