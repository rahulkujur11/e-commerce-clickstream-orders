import os
import datetime as dt
from typing import List

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

# ---------------------------
# Paths / config
# ---------------------------
DATA_ROOT = "/opt/data"
BRONZE_PATH = f"{DATA_ROOT}/bronze/events"

GOLD_ROOT = f"{DATA_ROOT}/gold"
GOLD_DAILY_METRICS = f"{GOLD_ROOT}/daily_metrics"
GOLD_PRODUCT_DAILY = f"{GOLD_ROOT}/product_daily"
GOLD_USER_DAILY = f"{GOLD_ROOT}/user_daily"

RUN_ID = os.environ.get("RUN_ID", dt.datetime.utcnow().strftime("%Y%m%d%H%M%S"))
CKPT_GOLD = f"{DATA_ROOT}/checkpoints/{RUN_ID}/gold"

# This matches what you wrote to bronze (Parquet carries schema, but for file streaming it's best to provide it)
LINE_ITEM = T.StructType([
    T.StructField("sku",   T.StringType()),
    T.StructField("qty",   T.IntegerType()),
    T.StructField("price", T.DoubleType()),
])

BRONZE_SCHEMA = T.StructType([
    T.StructField("event_type",   T.StringType()),
    T.StructField("event_id",     T.StringType()),
    T.StructField("user_id",      T.StringType()),
    T.StructField("order_id",     T.StringType()),
    T.StructField("ts",           T.TimestampType()),
    T.StructField("url",          T.StringType()),
    T.StructField("referrer",     T.StringType()),
    T.StructField("ua",           T.StringType()),
    T.StructField("session_hint", T.StringType()),
    T.StructField("sku",          T.StringType()),
    T.StructField("qty",          T.IntegerType()),
    T.StructField("price",        T.DoubleType()),
    T.StructField("total",        T.DoubleType()),
    T.StructField("line_items",   T.ArrayType(LINE_ITEM)),
    T.StructField("event_date",   T.DateType()),
    T.StructField("ingest_ts",    T.TimestampType()),
])

def spark() -> SparkSession:
    sp = (SparkSession.builder
          .appName("weekend-de-gold")
          .config("spark.sql.shuffle.partitions", "4")
          # allow dynamic partition overwrite from foreachBatch
          .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
          .getOrCreate())
    return sp

def overwrite_partitions(df: DataFrame, path: str, partition_col: str = "event_date") -> None:
    """
    Idempotent write: only partitions present in df are overwritten (dynamic mode).
    """
    (df.write
       .mode("overwrite")
       .format("parquet")
       .partitionBy(partition_col)
       .option("path", path)
       .save())

def build_daily_metrics(day_events: DataFrame) -> DataFrame:
    # Site-level funnel & revenue per day
    agg = (day_events
           .groupBy("event_date")
           .agg(
               F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
               F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
               F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).alias("orders"),
               F.countDistinct("user_id").alias("unique_users"),
               F.sum(F.when(F.col("event_type") == "order_placed", F.coalesce(F.col("total"), F.lit(0.0))).otherwise(0.0)).alias("revenue")
           ))
    return (agg
            .withColumn("aov", F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")).otherwise(F.lit(0.0)))
            .withColumn("orders_per_user", F.when(F.col("unique_users") > 0, F.col("orders") / F.col("unique_users")).otherwise(F.lit(0.0)))
            .withColumn("conversion_rate_users", F.when(F.col("unique_users") > 0, F.col("orders") / F.col("unique_users")).otherwise(F.lit(0.0)))
            )

def build_product_daily(day_events: DataFrame) -> DataFrame:
    # Carts
    carts = (day_events
             .filter(F.col("event_type") == "add_to_cart")
             .select("event_date", "user_id", "sku", "qty", "price")
             .groupBy("event_date", "sku")
             .agg(
                 F.count(F.lit(1)).alias("cart_events"),
                 F.sum(F.coalesce(F.col("qty"), F.lit(0))).alias("cart_qty"),
                 F.sum(F.coalesce(F.col("qty"), F.lit(0)) * F.coalesce(F.col("price"), F.lit(0.0))).alias("cart_revenue")
             ))

    # Orders (explode items)
    order_items = (day_events
                   .filter(F.col("event_type") == "order_placed")
                   .select("event_date", "order_id", "user_id", F.explode_outer("line_items").alias("li"))
                   .select(
                       "event_date",
                       "order_id",
                       "user_id",
                       F.col("li.sku").alias("sku"),
                       F.coalesce(F.col("li.qty"), F.lit(0)).alias("qty"),
                       F.coalesce(F.col("li.price"), F.lit(0.0)).alias("price")
                   ))

    sales = (order_items
             .groupBy("event_date", "sku")
             .agg(
                 F.countDistinct("order_id").alias("orders_with_sku"),
                 F.countDistinct("user_id").alias("unique_buyers"),
                 F.sum("qty").alias("units_sold"),
                 F.sum(F.col("qty") * F.col("price")).alias("item_revenue"),
                 F.avg(F.when(F.col("qty") > 0, F.col("price"))).alias("avg_selling_price")
             ))

    # Combine (full outer to keep SKUs that only appear in carts or only in sales)
    combined = (carts.alias("c")
                .join(sales.alias("s"), on=["event_date", "sku"], how="full_outer")
                .select(
                    F.coalesce(F.col("c.event_date"), F.col("s.event_date")).alias("event_date"),
                    F.coalesce(F.col("c.sku"), F.col("s.sku")).alias("sku"),
                    F.coalesce(F.col("cart_events"), F.lit(0)).alias("cart_events"),
                    F.coalesce(F.col("cart_qty"), F.lit(0)).alias("cart_qty"),
                    F.coalesce(F.col("cart_revenue"), F.lit(0.0)).alias("cart_revenue"),
                    F.coalesce(F.col("orders_with_sku"), F.lit(0)).alias("orders_with_sku"),
                    F.coalesce(F.col("unique_buyers"), F.lit(0)).alias("unique_buyers"),
                    F.coalesce(F.col("units_sold"), F.lit(0)).alias("units_sold"),
                    F.coalesce(F.col("item_revenue"), F.lit(0.0)).alias("item_revenue"),
                    F.coalesce(F.col("avg_selling_price"), F.lit(0.0)).alias("avg_selling_price"),
                ))
    return combined

def build_user_daily(day_events: DataFrame) -> DataFrame:
    # Per-user per-day engagement & revenue
    user_day = (day_events
                .groupBy("event_date", "user_id")
                .agg(
                    F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
                    F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_events"),
                    F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).alias("orders"),
                    F.sum(F.when(F.col("event_type") == "order_placed", F.coalesce(F.col("total"), F.lit(0.0))).otherwise(0.0)).alias("revenue"),
                    F.min(F.when(F.col("event_type") == "order_placed", F.col("ts"))).alias("first_order_ts"),
                    F.max("ts").alias("last_event_ts")
                ))
    return user_day

def foreach_batch(sp: SparkSession):
    def _fn(batch_df: DataFrame, epoch_id: int):
        # What partitions (days) changed in this micro-batch?
        touched = [r["event_date"] for r in batch_df.select("event_date").distinct().collect()]
        if not touched:
            print(f"[gold] epoch={epoch_id} no new dates; skipping")
            return

        # Reload full-day data from bronze for only those dates (partition pruning)
        day_events = (sp.read
                      .format("parquet")
                      .schema(BRONZE_SCHEMA)
                      .load(BRONZE_PATH)
                      .where(F.col("event_date").isin(touched)))

        # Build gold datasets
        daily_metrics = build_daily_metrics(day_events)
        product_daily = build_product_daily(day_events)
        user_daily    = build_user_daily(day_events)

        # Overwrite just the touched partitions (idempotent)
        overwrite_partitions(daily_metrics, GOLD_DAILY_METRICS)
        overwrite_partitions(product_daily, GOLD_PRODUCT_DAILY)
        overwrite_partitions(user_daily,    GOLD_USER_DAILY)

        print(f"[gold] epoch={epoch_id} wrote partitions for dates={','.join(str(d) for d in touched)}")
    return _fn

def main():
    sp = spark()

    # Stream the deduped bronze events (single source -> simpler & robust)
    events_stream = (sp.readStream
                       .format("parquet")
                       .schema(BRONZE_SCHEMA)
                       .option("maxFilesPerTrigger", 1)      # dev-friendly; tune as needed
                       .load(BRONZE_PATH))

    q = (events_stream
         .writeStream
         .queryName("gold_builder")
         .foreachBatch(foreach_batch(sp))
         .option("checkpointLocation", CKPT_GOLD)
         .trigger(processingTime="30 seconds")
         .start())

    sp.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
