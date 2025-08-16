

select
  cast(event_id as varchar)    as event_id,
  cast(user_id  as varchar)    as user_id,
  cast(ts       as timestamp)  as ts,
  url,
  referrer,
  ua,
  session_hint,
  cast(event_date as date)     as event_date,
  cast(ingest_ts  as timestamp) as ingest_ts
from read_parquet('/opt/data/silver/page_view/**/*.parquet')